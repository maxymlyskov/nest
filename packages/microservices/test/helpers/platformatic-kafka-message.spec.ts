import {
  toKafkaParserInput,
  toPlatformaticKafkaMessage,
} from '../../helpers/platformatic-kafka-message.js';

describe('platformatic kafka message helpers', () => {
  describe('toKafkaParserInput', () => {
    it('should turn the header map into a record', () => {
      const input = toKafkaParserInput({
        topic: 'topic',
        partition: 1,
        key: Buffer.from('key'),
        value: Buffer.from('value'),
        headers: new Map([[Buffer.from('h'), Buffer.from('v')]]),
        offset: 5n,
        timestamp: 7n,
        commit: () => {},
      });

      expect(input).toEqual({
        topic: 'topic',
        partition: 1,
        key: Buffer.from('key'),
        value: Buffer.from('value'),
        headers: { h: Buffer.from('v') },
        offset: '5',
        timestamp: '7',
      });
    });
  });

  describe('toPlatformaticKafkaMessage', () => {
    it('should turn key, value and headers into buffers', () => {
      const message = toPlatformaticKafkaMessage('topic', {
        key: 'key',
        value: '{"a":1}',
        headers: { h: 'v', n: 1, missing: undefined },
      });

      expect(message).toEqual({
        topic: 'topic',
        key: Buffer.from('key'),
        value: Buffer.from('{"a":1}'),
        headers: new Map([
          [Buffer.from('h'), Buffer.from('v')],
          [Buffer.from('n'), Buffer.from('1')],
        ]),
      });
    });

    it('should leave out a missing key and value', () => {
      const message = toPlatformaticKafkaMessage('topic', {
        key: null,
        value: null,
        headers: {},
      });

      expect(message).toEqual({ topic: 'topic', headers: new Map() });
    });
  });
});
