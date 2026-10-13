import { Controller } from '@nestjs/common';
import {
  Client,
  ClientPlatformaticKafka,
  Transport,
} from '@nestjs/microservices';
import { KafkaController } from './kafka.controller.js';

@Controller()
export class PlatformaticKafkaController extends KafkaController {
  @Client({
    transport: Transport.PLATFORMATIC_KAFKA,
    options: {
      client: {
        bootstrapBrokers: ['localhost:9092'],
      },
    },
  })
  protected readonly client: ClientPlatformaticKafka;
}
