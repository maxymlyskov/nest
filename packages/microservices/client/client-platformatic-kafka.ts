import { createRequire } from 'module';
import {
  throwError as _throw,
  connectable,
  defer,
  Observable,
  Subject,
} from 'rxjs';
import { mergeMap } from 'rxjs/operators';
import {
  KAFKA_DEFAULT_BROKER,
  KAFKA_DEFAULT_CLIENT,
  KAFKA_DEFAULT_GROUP,
} from '../constants.js';
import { KafkaResponseDeserializer } from '../deserializers/kafka-response.deserializer.js';
import { KafkaHeaders } from '../enums/index.js';
import { InvalidKafkaClientTopicException } from '../errors/invalid-kafka-client-topic.exception.js';
import { InvalidMessageException } from '../errors/invalid-message.exception.js';
import { KafkaStatus } from '../events/index.js';
import {
  Consumer,
  ConsumerGroupJoinPayload,
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
  encodeReplyPartitionAssignerUserData,
  PLATFORMATIC_KAFKA_REPLY_PROTOCOL,
  platformaticKafkaReplyPartitionAssigner,
} from '../helpers/platformatic-kafka-reply-partition-assigner.js';
import {
  IncomingResponse,
  MsPattern,
  OutgoingEvent,
  PlatformaticKafkaOptions,
  ReadPacket,
  WritePacket,
} from '../interfaces/index.js';
import {
  KafkaRequest,
  KafkaRequestSerializer,
} from '../serializers/kafka-request.serializer.js';
import { ClientProxy } from './client-proxy.js';
import { Logger } from '@nestjs/common';
import { isNil, isUndefined, loadPackage } from '@nestjs/common/internal';

/**
 * The consumer and producer created by a single `connect()` attempt, kept so a
 * failed attempt can close its own instances even after a newer attempt has
 * replaced them on the client.
 */
interface PlatformaticKafkaConnectionAttempt {
  consumer?: Consumer;
  producer?: Producer;
}

/**
 * A Kafka client built on `@platformatic/kafka`. It speaks the same wire
 * format as `ClientKafka` (headers, serializers, `.reply` topics), so it can
 * talk to either Kafka server.
 *
 * @publicApi
 */
export class ClientPlatformaticKafka extends ClientProxy<never, KafkaStatus> {
  protected logger = new Logger(ClientPlatformaticKafka.name);
  protected consumer: Consumer | null = null;
  protected producer: Producer | null = null;
  protected stream: MessagesStream | null = null;
  protected initialized: Promise<void> | null = null;
  protected parser: KafkaParser;
  protected responsePatterns: string[] = [];
  protected consumerAssignments: { [key: string]: number } = {};
  protected brokers: string[];
  protected clientId: string;
  protected groupId: string;
  protected producerOnlyMode: boolean;

  constructor(
    protected readonly options: Required<PlatformaticKafkaOptions>['options'],
  ) {
    super();

    const clientOptions = this.getOptionsProp(this.options, 'client', {});
    const consumerOptions = this.getOptionsProp(this.options, 'consumer', {});
    const postfixId = this.getOptionsProp(this.options, 'postfixId', '-client');
    this.producerOnlyMode = this.getOptionsProp(
      this.options,
      'producerOnlyMode',
      false,
    );

    this.brokers = (clientOptions.bootstrapBrokers as string[]) || [
      KAFKA_DEFAULT_BROKER,
    ];

    // Append a unique id to the clientId and groupId
    // so they don't collide with a microservices server
    this.clientId =
      (clientOptions.clientId || KAFKA_DEFAULT_CLIENT) + postfixId;
    this.groupId = (consumerOptions.groupId || KAFKA_DEFAULT_GROUP) + postfixId;
    this.parser = new KafkaParser(this.options?.parser);

    this.initializeSerializer(options);
    this.initializeDeserializer(options);
  }

  public subscribeToResponseOf(pattern: unknown): void {
    const request = this.normalizePattern(pattern as MsPattern);
    this.responsePatterns.push(this.getResponsePatternName(request));
  }

  public async connect(): Promise<Producer> {
    if (this.initialized) {
      return this.initialized.then(() => this.producer!);
    }
    const attempt: PlatformaticKafkaConnectionAttempt = {};
    const initialized = this.initializeClientAndConnections(attempt).catch(
      async err => {
        // A rejected attempt must not be cached: the next `connect()` call
        // has to try again once the broker is reachable.
        if (this.initialized === initialized) {
          this.consumer = null;
          this.producer = null;
          this.stream = null;
          this.initialized = null;
        }
        await Promise.allSettled([
          attempt.consumer?.close(),
          attempt.producer?.close(),
        ]);
        throw err;
      },
    );
    this.initialized = initialized;
    return initialized.then(() => this.producer!);
  }

  public async close(): Promise<void> {
    this.handleClose();
    const stream = this.stream;
    const consumer = this.consumer;
    const producer = this.producer;
    // Reset first, so a `connect()` during the close starts a new attempt
    // instead of reusing the instances being closed.
    this.stream = null;
    this.consumer = null;
    this.producer = null;
    this.initialized = null;
    // Pending requests were failed above, so a reconnecting client has no
    // reply partition left to keep.
    this.consumerAssignments = {};

    // The consumer cannot leave its group while a stream is still consuming.
    const results = [
      ...(await Promise.allSettled([stream?.close()])),
      ...(await Promise.allSettled([consumer?.close(), producer?.close()])),
    ];
    this._status$.next(KafkaStatus.STOPPED);

    const [failure, ...otherFailures] = results.filter(
      (result): result is PromiseRejectedResult => result.status === 'rejected',
    );
    otherFailures.forEach(({ reason }) => this.logger.error(reason));
    if (failure) {
      throw failure.reason;
    }
  }

  public handleClose() {
    if (this.routingMap.size > 0) {
      const err = new Error('Connection closed');
      const callbacks = [...this.routingMap.values()];
      this.routingMap.clear();

      for (const callback of callbacks) {
        try {
          callback({ err });
        } catch (callbackErr) {
          // A failing callback must not keep the remaining requests pending
          // nor prevent the connection from being closed.
          this.logger.error(callbackErr);
        }
      }
    }
  }

  public async loadKafkaPackage(): Promise<PlatformaticKafkaPackage> {
    return loadPackage(
      '@platformatic/kafka',
      ClientPlatformaticKafka.name,
      () => createRequire(import.meta.url)('@platformatic/kafka'),
    ) as Promise<PlatformaticKafkaPackage>;
  }

  private async initializeClientAndConnections(
    attempt: PlatformaticKafkaConnectionAttempt,
  ): Promise<void> {
    const kafkaPackage = await this.loadKafkaPackage();
    const clientOptions = {
      // kafkajs creates missing topics by default, @platformatic/kafka does not.
      autocreateTopics: true,
      ...this.options?.client,
      clientId: this.clientId,
      bootstrapBrokers: this.brokers,
    };

    if (!this.producerOnlyMode) {
      this.consumer = attempt.consumer = new kafkaPackage.Consumer({
        // Each member advertises the reply partitions it had, so the assigner
        // can keep them where in-flight replies are sent.
        protocols: [PLATFORMATIC_KAFKA_REPLY_PROTOCOL],
        protocolsMetadata: () =>
          encodeReplyPartitionAssignerUserData(this.consumerAssignments),
        partitionAssigner: platformaticKafkaReplyPartitionAssigner,
        ...this.options?.consumer,
        ...clientOptions,
        groupId: this.groupId,
      });
      this.registerConsumerEventListeners();
      await this.bindTopics();
    }

    this.producer = attempt.producer = new kafkaPackage.Producer({
      ...this.options?.producer,
      ...clientOptions,
    });
    this.registerProducerEventListeners();
  }

  public async bindTopics(): Promise<void> {
    if (!this.consumer) {
      throw Error('No consumer initialized');
    }
    if (this.responsePatterns.length === 0) {
      return;
    }

    // `consume()` joins the group first, so the reply partitions are known
    // once it resolves.
    this.stream = await this.consumer.consume({
      // The library defaults to "latest", which also skips what was produced
      // during a rebalance; kafkajs resumes from the committed offsets.
      mode: 'committed',
      ...(this.options?.consume || {}),
      topics: this.responsePatterns,
    });
    void this.consumeStream(this.stream);
  }

  protected async consumeStream(stream: MessagesStream) {
    try {
      for await (const message of stream) {
        await this.handleResponse(message).catch(err => this.logger.error(err));
      }
    } catch (err) {
      if (this.stream !== stream) {
        return;
      }
      this.logger.error(err);
      this._status$.next(KafkaStatus.CRASHED);
    }
  }

  public async handleResponse(message: Message): Promise<void> {
    const rawMessage = this.parser.parse<KafkaRequest>(
      toKafkaParserInput(message),
    );
    const correlationId = rawMessage.headers[KafkaHeaders.CORRELATION_ID];
    if (isNil(correlationId)) {
      return;
    }
    let incomingResponse: IncomingResponse;
    try {
      incomingResponse = await this.deserializer.deserialize(rawMessage);
    } catch (error) {
      // The correlation id is on the headers, so the pending request can be
      // failed here instead of staying pending forever.
      const callback = this.routingMap.get(correlationId.toString());
      return callback?.({ err: error, isDisposed: true });
    }
    const { err, response, isDisposed, id } = incomingResponse;
    const callback = this.routingMap.get(id);
    if (!callback) {
      return;
    }
    if (err || isDisposed) {
      return callback({
        err,
        response,
        isDisposed,
      });
    }
    callback({
      err,
      response,
    });
  }

  public getConsumerAssignments() {
    return this.consumerAssignments;
  }

  public emitBatch<TResult = unknown, TInput = unknown>(
    pattern: MsPattern,
    data: { messages: TInput[] },
  ): Observable<TResult> {
    if (isNil(pattern) || isNil(data)) {
      return _throw(() => new InvalidMessageException());
    }
    const source = defer(async () => this.connect()).pipe(
      mergeMap(
        () => this.dispatchBatchEvent({ pattern, data }) as Promise<TResult>,
      ),
    );
    const connectableSource = connectable(source, {
      connector: () => new Subject<TResult>(),
      resetOnDisconnect: false,
    });
    connectableSource.connect();
    return connectableSource as Observable<TResult>;
  }

  public unwrap<T>(): T {
    if (!this.producer) {
      throw new Error(
        'Not initialized. Please call the "connect" method first.',
      );
    }
    return [this.consumer, this.producer] as T;
  }

  public on<
    EventKey extends string | number | symbol = string | number | symbol,
    EventCallback = Function,
  >(event: EventKey, callback: EventCallback) {
    throw new Error('Method is not supported for Kafka client');
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
    // Set member assignments on join and rebalance
    this.consumer.on('consumer:group:join', payload =>
      this.setConsumerAssignments(payload),
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

  protected async dispatchBatchEvent<TInput = unknown>(
    packets: ReadPacket<{ messages: TInput[] }>,
  ): Promise<ProduceResult | undefined> {
    if (packets.data.messages.length === 0) {
      return;
    }
    const pattern = this.normalizePattern(packets.pattern);
    const outgoingEvents: KafkaRequest[] = await Promise.all(
      packets.data.messages.map(message =>
        this.serializer.serialize(message as ReadPacket['data'], { pattern }),
      ),
    );

    return this.producer!.send({
      ...(this.options?.send || {}),
      messages: outgoingEvents.map(event =>
        toPlatformaticKafkaMessage(pattern, event),
      ),
    });
  }

  protected async dispatchEvent<T = ProduceResult>(
    packet: OutgoingEvent,
  ): Promise<T> {
    const pattern = this.normalizePattern(packet.pattern);
    const outgoingEvent: KafkaRequest = await this.serializer.serialize(
      packet.data,
      { pattern },
    );

    const result = await this.producer!.send({
      ...(this.options?.send || {}),
      messages: [toPlatformaticKafkaMessage(pattern, outgoingEvent)],
    });
    return result as T;
  }

  protected getReplyTopicPartition(topic: string): string {
    const minimumPartition = this.consumerAssignments[topic];
    if (isUndefined(minimumPartition)) {
      throw new InvalidKafkaClientTopicException(topic);
    }

    // Get the minimum partition
    return minimumPartition.toString();
  }

  protected publish(
    partialPacket: ReadPacket,
    callback: (packet: WritePacket) => any,
  ): () => void {
    const packet = this.assignPacketId(partialPacket);
    this.routingMap.set(packet.id, callback);

    const cleanup = () => this.routingMap.delete(packet.id);
    const errorCallback = (err: unknown) => {
      cleanup();
      callback({ err });
    };

    try {
      const pattern = this.normalizePattern(partialPacket.pattern);
      const replyTopic = this.getResponsePatternName(pattern);
      const replyPartition = this.getReplyTopicPartition(replyTopic);

      Promise.resolve(this.serializer.serialize(packet.data, { pattern }))
        .then((serializedPacket: KafkaRequest) => {
          serializedPacket.headers[KafkaHeaders.CORRELATION_ID] = packet.id;
          serializedPacket.headers[KafkaHeaders.REPLY_TOPIC] = replyTopic;
          serializedPacket.headers[KafkaHeaders.REPLY_PARTITION] =
            replyPartition;

          return this.producer!.send({
            ...(this.options?.send || {}),
            messages: [toPlatformaticKafkaMessage(pattern, serializedPacket)],
          });
        })
        .catch(err => errorCallback(err));

      return cleanup;
    } catch (err) {
      errorCallback(err);
      return () => null;
    }
  }

  protected getResponsePatternName(pattern: string): string {
    return `${pattern}.reply`;
  }

  protected setConsumerAssignments(payload: ConsumerGroupJoinPayload): void {
    if (!payload.assignments) {
      return;
    }
    const consumerAssignments: { [key: string]: number } = {};

    for (const { topic, partitions } of payload.assignments) {
      if (!partitions.length) {
        continue;
      }
      // Pending replies are sent to the current reply partition, so it stays
      // the reply partition for as long as this member owns it, even when a
      // rebalance adds a lower one.
      const current = this.consumerAssignments[topic];
      consumerAssignments[topic] = partitions.includes(current)
        ? current
        : Math.min(...partitions);
    }

    this.consumerAssignments = consumerAssignments;
  }

  protected initializeSerializer(options: PlatformaticKafkaOptions['options']) {
    this.serializer = options?.serializer ?? new KafkaRequestSerializer();
  }

  protected initializeDeserializer(
    options: PlatformaticKafkaOptions['options'],
  ) {
    this.deserializer =
      options?.deserializer ?? new KafkaResponseDeserializer();
  }
}
