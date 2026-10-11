import { PlatformaticKafkaContext } from '../../ctx-host/index.js';
import {
  Consumer,
  Message,
  Producer,
} from '../../external/platformatic-kafka.interface.js';

describe('PlatformaticKafkaContext', () => {
  const args = [
    { test: 'message' },
    1,
    'test',
    { header: 'value' },
    { test: 'consumer' },
    { test: 'producer' },
  ];
  let context: PlatformaticKafkaContext;

  beforeEach(() => {
    context = new PlatformaticKafkaContext(
      args as [
        Message,
        number,
        string,
        Record<string, unknown>,
        Consumer,
        Producer,
      ],
    );
  });
  describe('getMessage', () => {
    it('should return original message', () => {
      expect(context.getMessage()).toEqual(args[0]);
    });
  });
  describe('getPartition', () => {
    it('should return partition', () => {
      expect(context.getPartition()).toEqual(args[1]);
    });
  });
  describe('getTopic', () => {
    it('should return topic', () => {
      expect(context.getTopic()).toEqual(args[2]);
    });
  });
  describe('getHeaders', () => {
    it('should return decoded headers', () => {
      expect(context.getHeaders()).toEqual(args[3]);
    });
  });
  describe('getConsumer', () => {
    it('should return consumer instance', () => {
      expect(context.getConsumer()).toEqual(args[4]);
    });
  });
  describe('getProducer', () => {
    it('should return producer instance', () => {
      expect(context.getProducer()).toEqual(args[5]);
    });
  });
});
