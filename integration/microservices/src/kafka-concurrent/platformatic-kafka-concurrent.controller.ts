import { Controller } from '@nestjs/common';
import {
  Client,
  ClientPlatformaticKafka,
  Transport,
} from '@nestjs/microservices';
import { KafkaConcurrentController } from './kafka-concurrent.controller.js';

/**
 * The following function explicitly sends messages to the partition named in
 * the "toPartition" header.
 */
const explicitPartitioner = (message: { headers?: Map<Buffer, Buffer> }) => {
  for (const [key, value] of message.headers ?? []) {
    if (key.toString() === 'toPartition') {
      return parseFloat(value.toString());
    }
  }
  return 0;
};

@Controller()
export class PlatformaticKafkaConcurrentController extends KafkaConcurrentController {
  @Client({
    transport: Transport.PLATFORMATIC_KAFKA,
    options: {
      client: {
        bootstrapBrokers: ['localhost:9092'],
      },
      producer: {
        partitioner: explicitPartitioner,
      },
    },
  })
  public readonly client: ClientPlatformaticKafka;
}
