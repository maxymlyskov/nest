import {
  Message,
  MessageToProduce,
} from '../external/platformatic-kafka.interface.js';
import { KafkaRequest } from '../serializers/kafka-request.serializer.js';
import { isNil } from '@nestjs/common/internal';

/**
 * The shape `KafkaParser` reads: kafkajs passes headers as a record, while
 * `@platformatic/kafka` hands them over as a `Map` of buffers.
 */
export function toKafkaParserInput(message: Message) {
  const headers: Record<string, Buffer> = {};
  for (const [key, value] of message.headers) {
    headers[key.toString()] = value;
  }
  return {
    key: message.key,
    value: message.value,
    headers,
    topic: message.topic,
    partition: message.partition,
    offset: message.offset.toString(),
    timestamp: message.timestamp.toString(),
  };
}

/**
 * Turns a serialized `KafkaRequest` into a message for `producer.send()`.
 * Without serializers the producer writes keys, values and headers as they
 * are, so all of them have to be buffers already.
 */
export function toPlatformaticKafkaMessage(
  topic: string,
  request: KafkaRequest,
): MessageToProduce {
  const headers = new Map<Buffer, Buffer>();
  for (const [key, value] of Object.entries(request.headers ?? {})) {
    if (!isNil(value)) {
      headers.set(Buffer.from(key), toBuffer(value));
    }
  }
  const message: MessageToProduce = { topic, headers };
  if (!isNil(request.key)) {
    message.key = toBuffer(request.key);
  }
  if (!isNil(request.value)) {
    message.value = toBuffer(request.value);
  }
  return message;
}

function toBuffer(value: Buffer | string | number): Buffer {
  return Buffer.isBuffer(value) ? value : Buffer.from(String(value));
}
