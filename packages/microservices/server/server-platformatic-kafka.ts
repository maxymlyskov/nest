import { createRequire } from 'module';
import {
  finalize,
  isObservable,
  lastValueFrom,
  Observable,
  ReplaySubject,
} from 'rxjs';
import {
  KAFKA_DEFAULT_BROKER,
  KAFKA_DEFAULT_CLIENT,
  KAFKA_DEFAULT_GROUP,
  NO_EVENT_HANDLER,
  NO_MESSAGE_HANDLER,
} from '../constants.js';
import { PlatformaticKafkaContext } from '../ctx-host/index.js';
import { KafkaRequestDeserializer } from '../deserializers/kafka-request.deserializer.js';
import { KafkaHeaders, Transport } from '../enums/index.js';
import { KafkaStatus } from '../events/index.js';
import { KafkaRetriableException } from '../exceptions/index.js';
import {
  Consumer,
  Message,
  MessagesStream,
  PlatformaticKafkaPackage,
  ProduceResult,
  Producer,
} from '../external/platformatic-kafka.interface.js';
import { KafkaParser } from '../helpers/index.js';
import {
  toKafkaParserInput,
  toPlatformaticKafkaMessage,
} from '../helpers/platformatic-kafka-message.js';
import {
  PacketId,
  PlatformaticKafkaOptions,
  ReadPacket,
  TransportId,
  WritePacket,
} from '../interfaces/index.js';
import { KafkaRequest } from '../serializers/kafka-request.serializer.js';
import { KafkaRequestSerializer } from '../serializers/kafka-request.serializer.js';
import { Server } from './server.js';
import { Logger } from '@nestjs/common';
import { isNil } from '@nestjs/common/internal';

type ReplyPacket = WritePacket & Partial<PacketId>;

/**
 * A Kafka transport built on `@platformatic/kafka`. It speaks the same wire
 * format as `ServerKafka` (headers, serializers, `.reply` topics).
 *
 * @publicApi
 */
export class ServerPlatformaticKafka extends Server<never, KafkaStatus> {
  // The consume loop awaits `handleMessage` and logs what it rejects with, so
  // a second report from Nest would duplicate it.
  public override readonly propagatesEventHandlerErrors = true;
  public transportId: TransportId = Transport.PLATFORMATIC_KAFKA;

  protected logger = new Logger(ServerPlatformaticKafka.name);
  protected consumer: Consumer | null = null;
  protected producer: Producer | null = null;
  protected stream: MessagesStream | null = null;
  protected parser: KafkaParser;
  protected brokers: string[];
  protected clientId: string;
  protected groupId: string;

  constructor(
    protected readonly options: Required<PlatformaticKafkaOptions>['options'],
  ) {
    super();

    const clientOptions = this.getOptionsProp(this.options, 'client', {});
    const consumerOptions = this.getOptionsProp(this.options, 'consumer', {});
    const postfixId = this.getOptionsProp(this.options, 'postfixId', '-server');

    this.brokers = (clientOptions.bootstrapBrokers as string[]) || [
      KAFKA_DEFAULT_BROKER,
    ];

    // Append a unique id to the clientId and groupId
    // so they don't collide with a microservices client
    this.clientId =
      (clientOptions.clientId || KAFKA_DEFAULT_CLIENT) + postfixId;
    this.groupId = (consumerOptions.groupId || KAFKA_DEFAULT_GROUP) + postfixId;

    this.parser = new KafkaParser(this.options?.parser);

    this.initializeSerializer(options);
    this.initializeDeserializer(options);
  }

  public async listen(
    callback: (err?: unknown, ...optionalParams: unknown[]) => void,
  ): Promise<void> {
    try {
      await this.start(callback);
    } catch (err) {
      callback(err);
    }
  }

  public async close(): Promise<void> {
    const stream = this.stream;
    const consumer = this.consumer;
    const producer = this.producer;
    this.stream = null;
    this.consumer = null;
    this.producer = null;

    // The consumer cannot leave its group while a stream is still consuming.
    await Promise.allSettled([stream?.close()]);
    await Promise.allSettled([consumer?.close(), producer?.close()]);
    this._status$.next(KafkaStatus.STOPPED);
  }

  public async start(callback: () => void): Promise<void> {
    const kafkaPackage = await this.loadKafkaPackage();
    const clientOptions = {
      ...this.options?.client,
      clientId: this.clientId,
      bootstrapBrokers: this.brokers,
    };
    this.consumer = new kafkaPackage.Consumer({
      ...this.options?.consumer,
      ...clientOptions,
      groupId: this.groupId,
    });
    this.producer = new kafkaPackage.Producer({
      ...this.options?.producer,
      ...clientOptions,
    });
    this.registerConsumerEventListeners();
    this.registerProducerEventListeners();

    await this.bindEvents(this.consumer);
    callback();
  }

  public async loadKafkaPackage(): Promise<PlatformaticKafkaPackage> {
    return this.loadPackage(
      '@platformatic/kafka',
      ServerPlatformaticKafka.name,
      () => createRequire(import.meta.url)('@platformatic/kafka'),
    ) as Promise<PlatformaticKafkaPackage>;
  }

  protected registerConsumerEventListeners() {
    if (!this.consumer) {
      return;
    }
    this.consumer.on('client:broker:connect', () =>
      this._status$.next(KafkaStatus.CONNECTED),
    );
    this.consumer.on('client:broker:disconnect', () =>
      this._status$.next(KafkaStatus.DISCONNECTED),
    );
    this.consumer.on('consumer:group:rebalance', () =>
      this._status$.next(KafkaStatus.REBALANCING),
    );
  }

  protected registerProducerEventListeners() {
    if (!this.producer) {
      return;
    }
    this.producer.on('client:broker:connect', () =>
      this._status$.next(KafkaStatus.CONNECTED),
    );
    this.producer.on('client:broker:disconnect', () =>
      this._status$.next(KafkaStatus.DISCONNECTED),
    );
  }

  public async bindEvents(consumer: Consumer) {
    const registeredPatterns = [...this.messageHandlers.keys()];
    if (registeredPatterns.length === 0) {
      return;
    }

    this.stream = await consumer.consume({
      ...(this.options?.consume || {}),
      topics: registeredPatterns,
    });
    void this.consumeStream(this.stream);
  }

  // One message at a time, in order, as kafkajs' `eachMessage` does by default.
  protected async consumeStream(stream: MessagesStream) {
    try {
      for await (const message of stream) {
        await this.handleMessage(message).catch(err => this.logger.error(err));
      }
    } catch (err) {
      if (this.stream !== stream) {
        return;
      }
      this.logger.error(err);
      this._status$.next(KafkaStatus.CRASHED);
    }
  }

  public getPublisher(
    replyTopic: string,
    replyPartition: string,
    correlationId: string,
    context: PlatformaticKafkaContext,
  ): (data: ReplyPacket) => Promise<ProduceResult> {
    return (data: ReplyPacket) =>
      this.sendMessage(
        data,
        replyTopic,
        replyPartition,
        correlationId,
        context,
      );
  }

  public async handleMessage(message: Message) {
    const channel = message.topic;
    const rawMessage = this.parser.parse<KafkaRequest>(
      toKafkaParserInput(message),
    );
    const decodedHeaders = rawMessage.headers as Record<string, string>;
    const correlationId = decodedHeaders[KafkaHeaders.CORRELATION_ID];
    const replyTopic = decodedHeaders[KafkaHeaders.REPLY_TOPIC];
    const replyPartition = decodedHeaders[KafkaHeaders.REPLY_PARTITION];

    const packet = await this.deserializer.deserialize(rawMessage, {
      channel,
    });
    const kafkaContext = new PlatformaticKafkaContext([
      message,
      message.partition,
      message.topic,
      decodedHeaders,
      this.consumer!,
      this.producer!,
    ]);
    const handler = this.getHandlerByPattern(packet.pattern);
    // if the correlation id or reply topic is not set
    // then this is an event (events could still have correlation id)
    if (handler?.isEventHandler || !correlationId || !replyTopic) {
      return this.handleEvent(packet.pattern, packet, kafkaContext);
    }

    const publish = this.getPublisher(
      replyTopic,
      replyPartition,
      correlationId,
      kafkaContext,
    );

    if (!handler) {
      return publish({
        id: correlationId,
        err: NO_MESSAGE_HANDLER,
      });
    }
    return this.handleRequest(
      kafkaContext,
      async () => {
        const response$ = this.transformToObservable(
          handler(packet.data, kafkaContext),
        );

        const replayStream$ = new ReplaySubject();
        await this.combineStreamsAndThrowIfRetriable(response$, replayStream$);
        return replayStream$;
      },
      publish,
    );
  }

  public unwrap<T>(): T {
    if (!this.consumer) {
      throw new Error(
        'Not initialized. Please call the "listen"/"startAllMicroservices" method before accessing the server.',
      );
    }
    return [this.consumer, this.producer] as T;
  }

  public on<
    EventKey extends string | number | symbol = string | number | symbol,
    EventCallback = Function,
  >(event: EventKey, callback: EventCallback) {
    throw new Error('Method is not supported for Kafka server');
  }

  private combineStreamsAndThrowIfRetriable(
    response$: Observable<unknown>,
    replayStream$: ReplaySubject<unknown>,
  ) {
    return new Promise<void>((resolve, reject) => {
      let isPromiseResolved = false;
      response$.subscribe({
        next: val => {
          replayStream$.next(val);
          if (!isPromiseResolved) {
            isPromiseResolved = true;
            resolve();
          }
        },
        error: err => {
          if (err instanceof KafkaRetriableException && !isPromiseResolved) {
            isPromiseResolved = true;
            reject(err);
          } else {
            resolve();
          }
          replayStream$.error(err);
        },
        complete: () => {
          replayStream$.complete();
          if (!isPromiseResolved) {
            isPromiseResolved = true;
            resolve();
          }
        },
      });
    });
  }

  public async sendMessage(
    message: ReplyPacket,
    replyTopic: string,
    replyPartition: string | undefined | null,
    correlationId: string,
    context: PlatformaticKafkaContext,
  ): Promise<ProduceResult> {
    const outgoingMessage: KafkaRequest = await this.serializer.serialize(
      message.response,
    );
    const headers: Record<string, unknown> = {
      ...outgoingMessage.headers,
      [KafkaHeaders.CORRELATION_ID]: correlationId,
    };
    if (message.err) {
      headers[KafkaHeaders.NEST_ERR] =
        typeof message.err === 'object'
          ? JSON.stringify(message.err)
          : String(message.err);
    }
    if (message.isDisposed) {
      headers[KafkaHeaders.NEST_IS_DISPOSED] = Buffer.alloc(1);
    }

    const replyMessage = toPlatformaticKafkaMessage(replyTopic, {
      ...outgoingMessage,
      headers,
    });
    if (!isNil(replyPartition)) {
      replyMessage.partition = parseFloat(replyPartition);
    }
    return this.producer!.send({
      ...(this.options?.send || {}),
      messages: [replyMessage],
    });
  }

  public async handleEvent(
    pattern: string,
    packet: ReadPacket,
    context: PlatformaticKafkaContext,
  ): Promise<void> {
    const handler = this.getHandlerByPattern(pattern);
    if (!handler) {
      return this.logger.error(NO_EVENT_HANDLER`${pattern}`);
    }

    return this.runWithProcessingHooks(context, async runEndHook => {
      const resultOrStream = await handler(packet.data, context);
      if (isObservable(resultOrStream)) {
        await lastValueFrom(resultOrStream.pipe(finalize(runEndHook)), {
          defaultValue: undefined,
        });
      } else {
        runEndHook();
      }
    });
  }

  protected initializeSerializer(options: PlatformaticKafkaOptions['options']) {
    this.serializer = options?.serializer ?? new KafkaRequestSerializer();
  }

  protected initializeDeserializer(
    options: PlatformaticKafkaOptions['options'],
  ) {
    this.deserializer = options?.deserializer ?? new KafkaRequestDeserializer();
  }
}
