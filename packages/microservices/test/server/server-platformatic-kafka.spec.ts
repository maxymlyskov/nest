import { Logger } from '@nestjs/common';
import { EMPTY, of, throwError } from 'rxjs';
import { NO_MESSAGE_HANDLER } from '../../constants.js';
import { PlatformaticKafkaContext } from '../../ctx-host/index.js';
import { KafkaHeaders } from '../../enums/index.js';
import { KafkaStatus } from '../../events/index.js';
import { KafkaRetriableException } from '../../exceptions/index.js';
import {
  Message,
  MessageToProduce,
  ProduceResult,
} from '../../external/platformatic-kafka.interface.js';
import { WritePacket } from '../../interfaces/index.js';
import { ServerPlatformaticKafka } from '../../server/index.js';
import { objectToMap } from './utils/object-to-map.js';

class NoopLogger extends Logger {
  log(message: any, context?: string): void {}
  error(message: any, trace?: string, context?: string): void {}
  warn(message: any, context?: string): void {}
}

describe('ServerPlatformaticKafka', () => {
  const topic = 'test.topic';
  const replyTopic = 'test.topic.reply';
  const replyPartition = '0';
  const correlationId = '696fa0a9-1827-4e59-baef-f3628173fe4f';
  const messageValue = 'test-message';

  const createMessage = (headers: Record<string, string> = {}): Message => ({
    topic,
    partition: 0,
    key: Buffer.from('1'),
    value: Buffer.from(messageValue),
    headers: new Map(
      Object.entries(headers).map(([k, v]) => [Buffer.from(k), Buffer.from(v)]),
    ),
    offset: 0n,
    timestamp: 0n,
    commit: () => {},
  });
  const requestMessage = createMessage({
    [KafkaHeaders.CORRELATION_ID]: correlationId,
    [KafkaHeaders.REPLY_TOPIC]: replyTopic,
    [KafkaHeaders.REPLY_PARTITION]: replyPartition,
  });

  const createStream = (messages: Message[], error?: Error) => ({
    close: vi.fn(async () => {}),
    async *[Symbol.asyncIterator]() {
      yield* messages;
      if (error) {
        throw error;
      }
    },
  });

  let server: ServerPlatformaticKafka;
  let untypedServer: any;
  let consumer: Record<string, ReturnType<typeof vi.fn>>;
  let producer: Record<string, ReturnType<typeof vi.fn>>;
  let ConsumerCtor: ReturnType<typeof vi.fn>;
  let ProducerCtor: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    server = new ServerPlatformaticKafka({});
    untypedServer = server as any;
    untypedServer.logger = new NoopLogger();

    consumer = {
      on: vi.fn(),
      consume: vi.fn(async () => createStream([])),
      close: vi.fn(async () => {}),
    };
    producer = {
      on: vi.fn(),
      send: vi.fn(async () => ({})),
      close: vi.fn(async () => {}),
    };
    ConsumerCtor = vi.fn(function () {
      return consumer;
    });
    ProducerCtor = vi.fn(function () {
      return producer;
    });
    vi.spyOn(server, 'loadKafkaPackage').mockResolvedValue({
      Consumer: ConsumerCtor,
      Producer: ProducerCtor,
    } as any);
  });

  describe('listen', () => {
    it('should create the consumer and producer with suffixed ids', async () => {
      await server.listen(() => {});

      expect(ConsumerCtor).toHaveBeenCalledWith(
        expect.objectContaining({
          clientId: 'nestjs-consumer-server',
          groupId: 'nestjs-group-server',
          bootstrapBrokers: ['localhost:9092'],
        }),
      );
      expect(ProducerCtor).toHaveBeenCalledWith(
        expect.objectContaining({
          clientId: 'nestjs-consumer-server',
          bootstrapBrokers: ['localhost:9092'],
        }),
      );
    });
    it('should pass client, consumer and producer options through', async () => {
      server = new ServerPlatformaticKafka({
        postfixId: '-svc',
        client: {
          clientId: 'app',
          bootstrapBrokers: ['kafka:9092'],
          retries: 5,
        },
        consumer: { groupId: 'orders', sessionTimeout: 10000 },
        producer: { acks: 1 },
      });
      vi.spyOn(server, 'loadKafkaPackage').mockResolvedValue({
        Consumer: ConsumerCtor,
        Producer: ProducerCtor,
      } as any);

      await server.listen(() => {});

      expect(ConsumerCtor).toHaveBeenCalledWith({
        clientId: 'app-svc',
        groupId: 'orders-svc',
        bootstrapBrokers: ['kafka:9092'],
        retries: 5,
        sessionTimeout: 10000,
      });
      expect(ProducerCtor).toHaveBeenCalledWith({
        clientId: 'app-svc',
        bootstrapBrokers: ['kafka:9092'],
        retries: 5,
        acks: 1,
      });
    });
    it('should call callback', async () => {
      const callback = vi.fn();
      await server.listen(callback);
      expect(callback).toHaveBeenCalledWith();
    });
    describe('when "start" throws an exception', () => {
      it('should call callback with a thrown error as an argument', async () => {
        const error = new Error('random error');
        const callback = vi.fn();
        vi.spyOn(server, 'start').mockRejectedValueOnce(error);

        await server.listen(callback);
        expect(callback).toHaveBeenCalledWith(error);
      });
    });
  });

  describe('status', () => {
    const emit = (
      target: Record<string, ReturnType<typeof vi.fn>>,
      event: string,
    ) => {
      const [, listener] = target.on.mock.calls.find(([e]) => e === event)!;
      listener();
    };

    it('should map consumer and producer events to kafka statuses', async () => {
      const statuses: KafkaStatus[] = [];
      server.status.subscribe(status => statuses.push(status));
      await server.listen(() => {});

      emit(consumer, 'client:broker:connect');
      emit(consumer, 'consumer:group:rebalance');
      emit(producer, 'client:broker:disconnect');

      expect(statuses).toEqual([
        KafkaStatus.CONNECTED,
        KafkaStatus.REBALANCING,
        KafkaStatus.DISCONNECTED,
      ]);
    });
  });

  describe('close', () => {
    it('should close the stream, consumer and producer', async () => {
      const stream = createStream([]);
      untypedServer.stream = stream;
      untypedServer.consumer = consumer;
      untypedServer.producer = producer;

      await server.close();

      expect(stream.close).toHaveBeenCalledOnce();
      expect(consumer.close).toHaveBeenCalledOnce();
      expect(producer.close).toHaveBeenCalledOnce();
      expect(untypedServer.stream).toBeNull();
      expect(untypedServer.consumer).toBeNull();
      expect(untypedServer.producer).toBeNull();
    });
    it('should close the stream before the consumer leaves its group', async () => {
      const calls: string[] = [];
      const stream = createStream([]);
      stream.close.mockImplementationOnce(async () => {
        await Promise.resolve();
        calls.push('stream');
      });
      consumer.close.mockImplementationOnce(async () => {
        calls.push('consumer');
      });
      untypedServer.stream = stream;
      untypedServer.consumer = consumer;

      await server.close();

      expect(calls).toEqual(['stream', 'consumer']);
    });
    it('should close the producer when the consumer fails to close', async () => {
      consumer.close.mockRejectedValueOnce(new Error('close failed'));
      untypedServer.consumer = consumer;
      untypedServer.producer = producer;

      await server.close();

      expect(producer.close).toHaveBeenCalledOnce();
      expect(untypedServer.consumer).toBeNull();
    });
    it('should emit "stopped"', async () => {
      const statuses: KafkaStatus[] = [];
      server.status.subscribe(status => statuses.push(status));

      await server.close();

      expect(statuses).toEqual([KafkaStatus.STOPPED]);
    });
  });

  describe('bindEvents', () => {
    it('should not consume when there are no message handlers', async () => {
      await server.bindEvents(consumer as any);
      expect(consumer.consume).not.toHaveBeenCalled();
    });
    it('should consume the registered patterns with the consume options', async () => {
      server = new ServerPlatformaticKafka({ consume: { mode: 'earliest' } });
      untypedServer = server as any;
      untypedServer.messageHandlers = objectToMap({
        [topic]: vi.fn(),
        'another.topic': vi.fn(),
      });

      await server.bindEvents(consumer as any);

      expect(consumer.consume).toHaveBeenCalledWith({
        mode: 'earliest',
        topics: [topic, 'another.topic'],
      });
    });
    it('should handle every streamed message in order', async () => {
      const first = createMessage();
      const second = createMessage();
      consumer.consume.mockResolvedValueOnce(createStream([first, second]));
      untypedServer.messageHandlers = objectToMap({ [topic]: vi.fn() });
      const handled: Message[] = [];
      vi.spyOn(server, 'handleMessage').mockImplementation(async message => {
        handled.push(message);
      });

      await server.bindEvents(consumer as any);

      await vi.waitFor(() => expect(handled).toEqual([first, second]));
    });
    it('should keep consuming after a handler rejects', async () => {
      const first = createMessage();
      const second = createMessage();
      consumer.consume.mockResolvedValueOnce(createStream([first, second]));
      untypedServer.messageHandlers = objectToMap({ [topic]: vi.fn() });
      const handleMessage = vi
        .spyOn(server, 'handleMessage')
        .mockRejectedValueOnce(new KafkaRetriableException('retry'))
        .mockResolvedValueOnce(undefined);

      await server.bindEvents(consumer as any);

      await vi.waitFor(() => expect(handleMessage).toHaveBeenCalledTimes(2));
    });
    it('should emit "crashed" when the stream fails', async () => {
      consumer.consume.mockResolvedValueOnce(
        createStream([], new Error('fetch failed')),
      );
      untypedServer.messageHandlers = objectToMap({ [topic]: vi.fn() });
      const statuses: KafkaStatus[] = [];
      server.status.subscribe(status => statuses.push(status));

      await server.bindEvents(consumer as any);

      await vi.waitFor(() => expect(statuses).toEqual([KafkaStatus.CRASHED]));
    });
  });

  describe('handleMessage', () => {
    let publish: ReturnType<
      typeof vi.fn<(data: WritePacket) => Promise<ProduceResult>>
    >;

    beforeEach(() => {
      publish = vi.fn(async () => ({}));
      vi.spyOn(server, 'getPublisher').mockImplementation(() => publish);
    });

    it('should call "handleEvent" if correlation identifier is not present', async () => {
      const handleEvent = vi
        .spyOn(server, 'handleEvent')
        .mockResolvedValueOnce(undefined);
      await server.handleMessage(createMessage());
      expect(handleEvent).toHaveBeenCalledWith(
        topic,
        { pattern: topic, data: messageValue },
        expect.any(PlatformaticKafkaContext),
      );
    });
    it('should call "handleEvent" if the reply topic is not present', async () => {
      const handleEvent = vi
        .spyOn(server, 'handleEvent')
        .mockResolvedValueOnce(undefined);
      await server.handleMessage(
        createMessage({ [KafkaHeaders.CORRELATION_ID]: correlationId }),
      );
      expect(handleEvent).toHaveBeenCalled();
    });
    it('should call "handleEvent" if the handler is an event handler', async () => {
      const handleEvent = vi
        .spyOn(server, 'handleEvent')
        .mockResolvedValueOnce(undefined);
      untypedServer.messageHandlers = objectToMap({
        [topic]: Object.assign(vi.fn(), { isEventHandler: true }),
      });
      await server.handleMessage(requestMessage);
      expect(handleEvent).toHaveBeenCalled();
    });
    it(`should publish NO_MESSAGE_HANDLER if pattern not exists in messageHandlers object`, async () => {
      await server.handleMessage(requestMessage);
      expect(publish).toHaveBeenCalledWith({
        id: correlationId,
        err: NO_MESSAGE_HANDLER,
      });
    });
    it('should call handler with the decoded value and a context', async () => {
      const handler = vi.fn(async () => 'response');
      untypedServer.messageHandlers = objectToMap({ [topic]: handler });

      await server.handleMessage(requestMessage);

      const [data, context] = handler.mock.calls[0] as unknown as [
        string,
        PlatformaticKafkaContext,
      ];
      expect(data).toBe(messageValue);
      expect(context.getMessage()).toBe(requestMessage);
      expect(context.getTopic()).toBe(topic);
      expect(context.getHeaders()).toEqual({
        [KafkaHeaders.CORRELATION_ID]: correlationId,
        [KafkaHeaders.REPLY_TOPIC]: replyTopic,
        [KafkaHeaders.REPLY_PARTITION]: replyPartition,
      });
      expect(context.getConsumer()).toBe(untypedServer.consumer);
    });
    it('should publish the response to the reply topic and partition', async () => {
      untypedServer.messageHandlers = objectToMap({
        [topic]: vi.fn(async () => 'response'),
      });
      const getPublisher = server.getPublisher as unknown as ReturnType<
        typeof vi.fn
      >;

      await server.handleMessage(requestMessage);

      expect(getPublisher).toHaveBeenCalledWith(
        replyTopic,
        replyPartition,
        correlationId,
        expect.any(PlatformaticKafkaContext),
      );
      await vi.waitFor(() =>
        expect(publish).toHaveBeenCalledWith(
          expect.objectContaining({ response: 'response' }),
        ),
      );
    });
    it('should reject when the handler fails with a retriable exception', async () => {
      untypedServer.messageHandlers = objectToMap({
        [topic]: vi.fn(async () =>
          throwError(() => new KafkaRetriableException('retry')),
        ),
      });

      await expect(server.handleMessage(requestMessage)).rejects.toThrow(
        KafkaRetriableException,
      );
    });
  });

  describe('handleEvent', () => {
    const context = new PlatformaticKafkaContext([] as any);

    function bindHandler(handler: (...args: any[]) => unknown) {
      const endHook = vi.fn();
      untypedServer.onProcessingStartHook = (
        _transportId: unknown,
        _ctx: unknown,
        fn: () => Promise<void>,
      ) => fn();
      untypedServer.onProcessingEndHook = endHook;
      untypedServer.messageHandlers = objectToMap({
        [topic]: Object.assign(handler, { isEventHandler: true }),
      });
      return endHook;
    }

    it('should run the end hook when the handler returns a plain value', async () => {
      const endHook = bindHandler(async () => 'plain');
      await server.handleEvent(topic, { pattern: topic, data: null }, context);
      expect(endHook).toHaveBeenCalledOnce();
    });
    it('should run the end hook when the returned stream completes without emitting', async () => {
      const endHook = bindHandler(async () => EMPTY);
      await server.handleEvent(topic, { pattern: topic, data: null }, context);
      expect(endHook).toHaveBeenCalledOnce();
    });
    it('should run the end hook once per event', async () => {
      const endHook = bindHandler(async () => of('first', 'second'));
      await server.handleEvent(topic, { pattern: topic, data: null }, context);
      expect(endHook).toHaveBeenCalledOnce();
    });
    it('should reject when the handler throws an error', async () => {
      const endHook = bindHandler(async () => {
        throw new Error('handler failed');
      });
      await expect(
        server.handleEvent(topic, { pattern: topic, data: null }, context),
      ).rejects.toThrow('handler failed');
      expect(endHook).toHaveBeenCalledOnce();
    });
  });

  describe('sendMessage', () => {
    const context = new PlatformaticKafkaContext([] as any);
    const headersOf = (message: MessageToProduce) =>
      Object.fromEntries(
        [...message.headers!].map(([key, value]) => [key.toString(), value]),
      );

    beforeEach(() => {
      untypedServer.producer = producer;
    });

    it('should send the response with buffers and the reply partition', async () => {
      await server.sendMessage(
        { id: correlationId, response: messageValue },
        replyTopic,
        replyPartition,
        correlationId,
        context,
      );

      expect(producer.send).toHaveBeenCalledWith({
        messages: [
          {
            topic: replyTopic,
            partition: 0,
            value: Buffer.from(messageValue),
            headers: new Map([
              [
                Buffer.from(KafkaHeaders.CORRELATION_ID),
                Buffer.from(correlationId),
              ],
            ]),
          },
        ],
      });
    });
    it('should send message without reply partition', async () => {
      await server.sendMessage(
        { id: correlationId, response: messageValue },
        replyTopic,
        undefined,
        correlationId,
        context,
      );

      const [{ messages }] = producer.send.mock.calls[0];
      expect(messages[0]).not.toHaveProperty('partition');
    });
    it('should send error message', async () => {
      await server.sendMessage(
        { id: correlationId, err: { message: 'failed' } },
        replyTopic,
        replyPartition,
        correlationId,
        context,
      );

      const [{ messages }] = producer.send.mock.calls[0];
      expect(headersOf(messages[0])[KafkaHeaders.NEST_ERR]).toEqual(
        Buffer.from(JSON.stringify({ message: 'failed' })),
      );
      expect(messages[0]).not.toHaveProperty('value');
    });
    it('should send `isDisposed` message', async () => {
      await server.sendMessage(
        { id: correlationId, isDisposed: true },
        replyTopic,
        replyPartition,
        correlationId,
        context,
      );

      const [{ messages }] = producer.send.mock.calls[0];
      expect(headersOf(messages[0])[KafkaHeaders.NEST_IS_DISPOSED]).toEqual(
        Buffer.alloc(1),
      );
    });
    it('should pass the send options', async () => {
      server = new ServerPlatformaticKafka({ send: { acks: -1 } });
      untypedServer = server as any;
      untypedServer.producer = producer;

      await server.sendMessage(
        { id: correlationId, response: messageValue },
        replyTopic,
        replyPartition,
        correlationId,
        context,
      );

      expect(producer.send).toHaveBeenCalledWith(
        expect.objectContaining({ acks: -1 }),
      );
    });
  });

  describe('unwrap', () => {
    it('should throw before "listen"', () => {
      expect(() => server.unwrap()).toThrow('Not initialized');
    });
    it('should return the consumer and producer', async () => {
      await server.listen(() => {});
      expect(server.unwrap()).toEqual([consumer, producer]);
    });
  });
});
