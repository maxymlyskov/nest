import { Logger } from '@nestjs/common';
import { lastValueFrom } from 'rxjs';
import { ClientPlatformaticKafka } from '../../client/client-platformatic-kafka.js';
import { KafkaHeaders } from '../../enums/index.js';
import { InvalidKafkaClientTopicException } from '../../errors/invalid-kafka-client-topic.exception.js';
import { InvalidMessageException } from '../../errors/invalid-message.exception.js';
import { KafkaStatus } from '../../events/index.js';
import {
  Message,
  MessageToProduce,
} from '../../external/platformatic-kafka.interface.js';
import {
  PLATFORMATIC_KAFKA_REPLY_PROTOCOL,
  platformaticKafkaReplyPartitionAssigner,
} from '../../helpers/platformatic-kafka-reply-partition-assigner.js';

class NoopLogger extends Logger {
  log(message: any, context?: string): void {}
  error(message: any, trace?: string, context?: string): void {}
  warn(message: any, context?: string): void {}
}

describe('ClientPlatformaticKafka', () => {
  const topic = 'test.topic';
  const replyTopic = 'test.topic.reply';
  const correlationId = '696fa0a9-1827-4e59-baef-f3628173fe4f';

  const createMessage = (
    headers: Record<string, string>,
    value = 'response',
  ): Message => ({
    topic: replyTopic,
    partition: 0,
    key: Buffer.from('1'),
    value: Buffer.from(value),
    headers: new Map(
      Object.entries(headers).map(([k, v]) => [Buffer.from(k), Buffer.from(v)]),
    ),
    offset: 0n,
    timestamp: 0n,
    commit: () => {},
  });
  const toHeaderRecord = (message: MessageToProduce) =>
    Object.fromEntries(
      [...message.headers!].map(([key, value]) => [
        key.toString(),
        value.toString(),
      ]),
    );

  const createStream = (messages: Message[] = [], error?: Error) => ({
    close: vi.fn(async () => {}),
    async *[Symbol.asyncIterator]() {
      yield* messages;
      if (error) {
        throw error;
      }
    },
  });

  let client: ClientPlatformaticKafka;
  let untypedClient: any;
  let consumer: Record<string, ReturnType<typeof vi.fn>>;
  let producer: Record<string, ReturnType<typeof vi.fn>>;
  let ConsumerCtor: ReturnType<typeof vi.fn>;
  let ProducerCtor: ReturnType<typeof vi.fn>;

  const createClient = (options = {}) => {
    client = new ClientPlatformaticKafka(options);
    untypedClient = client as any;
    untypedClient.logger = new NoopLogger();
    vi.spyOn(client, 'loadKafkaPackage').mockResolvedValue({
      Consumer: ConsumerCtor,
      Producer: ProducerCtor,
    } as any);
  };

  beforeEach(() => {
    consumer = {
      on: vi.fn(),
      consume: vi.fn(async () => createStream()),
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
    createClient();
  });

  describe('constructor', () => {
    it('should append the postfix to the client and group ids', () => {
      createClient({
        client: { clientId: 'app', bootstrapBrokers: ['broker:9092'] },
        consumer: { groupId: 'group' },
      });

      expect(untypedClient.clientId).toBe('app-client');
      expect(untypedClient.groupId).toBe('group-client');
      expect(untypedClient.brokers).toEqual(['broker:9092']);
    });
  });

  describe('subscribeToResponseOf', () => {
    it('should add the reply topic of the pattern', () => {
      client.subscribeToResponseOf(topic);
      client.subscribeToResponseOf({ cmd: 'sum' });

      expect(untypedClient.responsePatterns).toEqual([
        replyTopic,
        '{"cmd":"sum"}.reply',
      ]);
    });
  });

  describe('connect', () => {
    it('should create the consumer with the reply partition assigner', async () => {
      await client.connect();

      expect(ConsumerCtor).toHaveBeenCalledWith(
        expect.objectContaining({
          clientId: 'nestjs-consumer-client',
          groupId: 'nestjs-group-client',
          protocols: [PLATFORMATIC_KAFKA_REPLY_PROTOCOL],
          partitionAssigner: platformaticKafkaReplyPartitionAssigner,
        }),
      );
      expect(ProducerCtor).toHaveBeenCalledWith(
        expect.objectContaining({ clientId: 'nestjs-consumer-client' }),
      );
    });

    it('should advertise its current reply partitions', async () => {
      await client.connect();
      untypedClient.consumerAssignments = { [replyTopic]: 2 };

      const { protocolsMetadata } = ConsumerCtor.mock.calls[0][0];

      expect(JSON.parse(protocolsMetadata().toString())).toEqual({
        previousAssignment: { [replyTopic]: 2 },
      });
    });

    it('should consume the reply topics with the consume options', async () => {
      createClient({ consume: { mode: 'latest' } });
      client.subscribeToResponseOf(topic);

      await client.connect();

      expect(consumer.consume).toHaveBeenCalledWith({
        mode: 'latest',
        topics: [replyTopic],
      });
    });

    it('should not consume when no response pattern is subscribed', async () => {
      await client.connect();

      expect(consumer.consume).not.toHaveBeenCalled();
    });

    it('should reuse the connection', async () => {
      const first = await client.connect();
      const second = await client.connect();

      expect(first).toBe(producer);
      expect(second).toBe(producer);
      expect(ConsumerCtor).toHaveBeenCalledTimes(1);
      expect(ProducerCtor).toHaveBeenCalledTimes(1);
    });

    describe('when an attempt fails', () => {
      beforeEach(() => {
        client.subscribeToResponseOf(topic);
        consumer.consume.mockRejectedValueOnce(new Error('broker down'));
      });

      it('should close what the attempt created', async () => {
        await expect(client.connect()).rejects.toThrow('broker down');

        expect(consumer.close).toHaveBeenCalled();
        expect(untypedClient.consumer).toBeNull();
        expect(untypedClient.initialized).toBeNull();
      });

      it('should try again on the next call', async () => {
        await expect(client.connect()).rejects.toThrow('broker down');

        await expect(client.connect()).resolves.toBe(producer);
        expect(ConsumerCtor).toHaveBeenCalledTimes(2);
      });
    });

    describe('in producer only mode', () => {
      it('should not create a consumer', async () => {
        createClient({ producerOnlyMode: true });
        client.subscribeToResponseOf(topic);

        await client.connect();

        expect(ConsumerCtor).not.toHaveBeenCalled();
        expect(ProducerCtor).toHaveBeenCalled();
      });
    });
  });

  describe('status', () => {
    it('should follow the consumer and producer events', async () => {
      const statuses: string[] = [];
      client.status.subscribe(status => statuses.push(status));
      await client.connect();

      const listeners = new Map(
        [...consumer.on.mock.calls, ...producer.on.mock.calls].map(
          ([event, listener]) => [event, listener],
        ),
      );
      listeners.get('client:broker:connect')();
      listeners.get('consumer:group:rebalance')();
      listeners.get('client:broker:disconnect')();

      expect(statuses).toEqual([
        KafkaStatus.CONNECTED,
        KafkaStatus.REBALANCING,
        KafkaStatus.DISCONNECTED,
      ]);
    });

    describe('when the stream fails', () => {
      it('should report the client as crashed', async () => {
        consumer.consume.mockResolvedValueOnce(
          createStream([], new Error('fetch failed')),
        );
        const statuses: string[] = [];
        client.status.subscribe(status => statuses.push(status));
        client.subscribeToResponseOf(topic);

        await client.connect();

        await vi.waitFor(() => expect(statuses).toContain(KafkaStatus.CRASHED));
      });
    });
  });

  describe('setConsumerAssignments', () => {
    it('should keep the minimum partition per topic', () => {
      untypedClient.setConsumerAssignments({
        groupId: 'group',
        memberId: 'member',
        assignments: [
          { topic: replyTopic, partitions: [3, 1, 2] },
          { topic: 'other.reply', partitions: [] },
        ],
      });

      expect(client.getConsumerAssignments()).toEqual({ [replyTopic]: 1 });
    });

    it('should be called when the consumer joins the group', async () => {
      await client.connect();
      const [, onJoin] = consumer.on.mock.calls.find(
        ([event]) => event === 'consumer:group:join',
      )!;

      onJoin({
        groupId: 'group',
        memberId: 'member',
        assignments: [{ topic: replyTopic, partitions: [4] }],
      });

      expect(client.getConsumerAssignments()).toEqual({ [replyTopic]: 4 });
    });

    describe('when the current reply partition is still assigned', () => {
      it('should keep it, even when a lower partition was added', () => {
        untypedClient.consumerAssignments = { [replyTopic]: 1 };

        untypedClient.setConsumerAssignments({
          groupId: 'group',
          memberId: 'member',
          assignments: [{ topic: replyTopic, partitions: [0, 1] }],
        });

        expect(client.getConsumerAssignments()).toEqual({ [replyTopic]: 1 });
      });
    });

    describe('when the current reply partition was taken away', () => {
      it('should move to the minimum assigned partition', () => {
        untypedClient.consumerAssignments = { [replyTopic]: 1 };

        untypedClient.setConsumerAssignments({
          groupId: 'group',
          memberId: 'member',
          assignments: [{ topic: replyTopic, partitions: [2, 3] }],
        });

        expect(client.getConsumerAssignments()).toEqual({ [replyTopic]: 2 });
      });
    });

    describe('when the join has no assignments', () => {
      it('should keep the current ones', () => {
        untypedClient.consumerAssignments = { [replyTopic]: 1 };

        untypedClient.setConsumerAssignments({
          groupId: 'group',
          memberId: 'member',
        });

        expect(client.getConsumerAssignments()).toEqual({ [replyTopic]: 1 });
      });
    });
  });

  describe('getReplyTopicPartition', () => {
    it('should return the minimum partition', () => {
      untypedClient.consumerAssignments = { [replyTopic]: 3 };

      expect(untypedClient.getReplyTopicPartition(replyTopic)).toBe('3');
    });

    it('should throw when the topic is not assigned', () => {
      expect(() => untypedClient.getReplyTopicPartition(replyTopic)).toThrow(
        InvalidKafkaClientTopicException,
      );
    });
  });

  describe('handleResponse', () => {
    let callback: ReturnType<typeof vi.fn>;

    beforeEach(() => {
      callback = vi.fn();
      untypedClient.routingMap.set(correlationId, callback);
    });

    it('should call the callback with the response', async () => {
      await client.handleResponse(
        createMessage({ [KafkaHeaders.CORRELATION_ID]: correlationId }),
      );

      expect(callback).toHaveBeenCalledWith({
        err: undefined,
        response: 'response',
      });
    });

    it('should call the callback with the dispose flag', async () => {
      await client.handleResponse(
        createMessage({
          [KafkaHeaders.CORRELATION_ID]: correlationId,
          [KafkaHeaders.NEST_IS_DISPOSED]: '1',
        }),
      );

      expect(callback).toHaveBeenCalledWith({
        err: undefined,
        response: 'response',
        isDisposed: true,
      });
    });

    it('should call the callback with the error', async () => {
      await client.handleResponse(
        createMessage({
          [KafkaHeaders.CORRELATION_ID]: correlationId,
          [KafkaHeaders.NEST_ERR]: 'failed',
        }),
      );

      expect(callback).toHaveBeenCalledWith({
        err: 'failed',
        response: undefined,
        isDisposed: true,
      });
    });

    it('should ignore a message without a correlation id', async () => {
      await client.handleResponse(createMessage({}));

      expect(callback).not.toHaveBeenCalled();
    });

    it('should ignore a response for an unknown request', async () => {
      await client.handleResponse(
        createMessage({ [KafkaHeaders.CORRELATION_ID]: 'unknown' }),
      );

      expect(callback).not.toHaveBeenCalled();
    });

    describe('when the deserializer throws', () => {
      it('should fail the pending request', async () => {
        const error = new Error('cannot deserialize');
        vi.spyOn(untypedClient.deserializer, 'deserialize').mockImplementation(
          () => {
            throw error;
          },
        );

        await client.handleResponse(
          createMessage({ [KafkaHeaders.CORRELATION_ID]: correlationId }),
        );

        expect(callback).toHaveBeenCalledWith({
          err: error,
          isDisposed: true,
        });
      });
    });

    it('should be fed by the reply stream', async () => {
      consumer.consume.mockResolvedValueOnce(
        createStream([
          createMessage({ [KafkaHeaders.CORRELATION_ID]: correlationId }),
        ]),
      );
      client.subscribeToResponseOf(topic);

      await client.connect();

      await vi.waitFor(() => expect(callback).toHaveBeenCalled());
    });
  });

  describe('publish', () => {
    beforeEach(async () => {
      client.subscribeToResponseOf(topic);
      await client.connect();
      untypedClient.consumerAssignments = { [replyTopic]: 2 };
    });

    it('should send the request with the reply headers', async () => {
      untypedClient.publish({ pattern: topic, data: { a: 1 } }, vi.fn());

      await vi.waitFor(() => expect(producer.send).toHaveBeenCalled());
      const [{ messages }] = producer.send.mock.calls[0];
      const headers = toHeaderRecord(messages[0]);
      expect(messages[0].topic).toBe(topic);
      expect(messages[0].value.toString()).toBe('{"a":1}');
      expect(headers[KafkaHeaders.CORRELATION_ID]).toEqual(expect.any(String));
      expect(headers[KafkaHeaders.REPLY_TOPIC]).toBe(replyTopic);
      expect(headers[KafkaHeaders.REPLY_PARTITION]).toBe('2');
      expect(
        untypedClient.routingMap.has(headers[KafkaHeaders.CORRELATION_ID]),
      ).toBe(true);
    });

    it('should resolve "send" with the reply', async () => {
      const response = lastValueFrom(client.send(topic, { a: 1 }));
      await vi.waitFor(() => expect(producer.send).toHaveBeenCalled());
      const [{ messages }] = producer.send.mock.calls[0];
      const id = toHeaderRecord(messages[0])[KafkaHeaders.CORRELATION_ID];

      await client.handleResponse(
        createMessage(
          {
            [KafkaHeaders.CORRELATION_ID]: id,
            [KafkaHeaders.NEST_IS_DISPOSED]: '1',
          },
          '6',
        ),
      );

      await expect(response).resolves.toBe('6');
    });

    it('should remove the callback when unsubscribed', () => {
      const cleanup = untypedClient.publish(
        { pattern: topic, data: {} },
        vi.fn(),
      );

      cleanup();

      expect(untypedClient.routingMap.size).toBe(0);
    });

    describe('when the reply topic is not assigned', () => {
      it('should call the callback with the error', () => {
        untypedClient.consumerAssignments = {};
        const callback = vi.fn();

        untypedClient.publish({ pattern: topic, data: {} }, callback);

        expect(callback).toHaveBeenCalledWith({
          err: expect.any(InvalidKafkaClientTopicException),
        });
        expect(untypedClient.routingMap.size).toBe(0);
      });
    });

    describe('when the producer fails', () => {
      it('should call the callback with the error', async () => {
        const error = new Error('send failed');
        producer.send.mockRejectedValueOnce(error);
        const callback = vi.fn();

        untypedClient.publish({ pattern: topic, data: {} }, callback);

        await vi.waitFor(() =>
          expect(callback).toHaveBeenCalledWith({ err: error }),
        );
        expect(untypedClient.routingMap.size).toBe(0);
      });
    });
  });

  describe('dispatchEvent', () => {
    it('should send the event to the pattern topic', async () => {
      await client.connect();

      await lastValueFrom(client.emit(topic, { key: 'k', value: 'v' }));

      const [{ messages }] = producer.send.mock.calls[0];
      expect(messages).toEqual([
        {
          topic,
          key: Buffer.from('k'),
          value: Buffer.from('v'),
          headers: new Map(),
        },
      ]);
    });

    it('should pass the send options', async () => {
      createClient({ send: { acks: 1 } });
      await client.connect();

      await lastValueFrom(client.emit(topic, 'v'));

      expect(producer.send).toHaveBeenCalledWith(
        expect.objectContaining({ acks: 1 }),
      );
    });
  });

  describe('emitBatch', () => {
    it('should send every message in one call', async () => {
      await lastValueFrom(
        client.emitBatch(topic, { messages: ['a', { value: 'b' }] }),
      );

      const [{ messages }] = producer.send.mock.calls[0];
      expect(
        messages.map((message: MessageToProduce) => message.value!.toString()),
      ).toEqual(['a', 'b']);
    });

    it('should not send an empty batch', async () => {
      await lastValueFrom(client.emitBatch(topic, { messages: [] }), {
        defaultValue: undefined,
      });

      expect(producer.send).not.toHaveBeenCalled();
    });

    it('should fail without a pattern', async () => {
      await expect(
        lastValueFrom(client.emitBatch(null!, { messages: [] })),
      ).rejects.toBeInstanceOf(InvalidMessageException);
    });
  });

  describe('close', () => {
    beforeEach(async () => {
      client.subscribeToResponseOf(topic);
      await client.connect();
    });

    it('should close the stream, consumer and producer', async () => {
      const stream = untypedClient.stream;

      await client.close();

      expect(stream.close).toHaveBeenCalled();
      expect(consumer.close).toHaveBeenCalled();
      expect(producer.close).toHaveBeenCalled();
      expect(untypedClient.initialized).toBeNull();
    });

    it('should close the stream before the consumer leaves its group', async () => {
      const calls: string[] = [];
      untypedClient.stream.close.mockImplementationOnce(async () => {
        await Promise.resolve();
        calls.push('stream');
      });
      consumer.close.mockImplementationOnce(async () => {
        calls.push('consumer');
      });

      await client.close();

      expect(calls).toEqual(['stream', 'consumer']);
    });

    it('should fail pending requests with a connection closed error', async () => {
      const callback = vi.fn();
      untypedClient.routingMap.set(correlationId, callback);

      await client.close();

      expect(callback).toHaveBeenCalledWith({
        err: expect.objectContaining({ message: 'Connection closed' }),
      });
    });

    describe('when the producer fails to close', () => {
      it('should still close the consumer and throw', async () => {
        const error = new Error('close failed');
        producer.close.mockRejectedValueOnce(error);

        await expect(client.close()).rejects.toBe(error);
        expect(consumer.close).toHaveBeenCalled();
        expect(untypedClient.producer).toBeNull();
      });
    });

    it('should forget the reply partitions', async () => {
      untypedClient.consumerAssignments = { [replyTopic]: 1 };

      await client.close();

      expect(client.getConsumerAssignments()).toEqual({});
    });

    it('should report the client as stopped', async () => {
      const statuses: string[] = [];
      client.status.subscribe(status => statuses.push(status));

      await client.close();

      expect(statuses).toContain(KafkaStatus.STOPPED);
    });
  });

  describe('unwrap', () => {
    it('should throw when not connected', () => {
      expect(() => client.unwrap()).toThrow('Not initialized');
    });

    it('should return the consumer and producer', async () => {
      await client.connect();

      expect(client.unwrap()).toEqual([consumer, producer]);
    });
  });
});
