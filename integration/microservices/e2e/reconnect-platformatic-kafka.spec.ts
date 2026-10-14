import { Controller, INestApplication } from '@nestjs/common';
import {
  ClientPlatformaticKafka,
  KafkaStatus,
  MessagePattern,
  MicroserviceOptions,
  Payload,
  Transport,
} from '@nestjs/microservices';
import { Test } from '@nestjs/testing';
import { execFileSync } from 'child_process';
import { lastValueFrom, timeout } from 'rxjs';

@Controller()
class ReconnectController {
  @MessagePattern('reconnect.sum')
  sum(@Payload() data: number[]) {
    return data.reduce((a, b) => a + b, 0);
  }
}

/**
 * Skip this flaky test in CI/CD pipeline as it frequently
 * fails to connect to Kafka container in the cloud.
 * Restarts the `kafka` service of `integration/docker-compose.yml`.
 */
describe.skip('Kafka transport (@platformatic/kafka) broker restart', () => {
  let app: INestApplication;
  let client: ClientPlatformaticKafka;
  const serverStatuses: string[] = [];
  const clientStatuses: string[] = [];

  const sum = (data: number[]) =>
    lastValueFrom(client.send('reconnect.sum', data).pipe(timeout(10_000)));

  beforeAll(async () => {
    const module = await Test.createTestingModule({
      controllers: [ReconnectController],
    }).compile();

    app = module.createNestApplication();
    const microservice = app.connectMicroservice<MicroserviceOptions>({
      transport: Transport.PLATFORMATIC_KAFKA,
      options: {
        client: {
          bootstrapBrokers: ['localhost:9092'],
        },
      },
    });
    microservice.status.subscribe(status => serverStatuses.push(status));
    await app.startAllMicroservices();
    await app.init();

    client = new ClientPlatformaticKafka({
      client: {
        bootstrapBrokers: ['localhost:9092'],
      },
    });
    client.status.subscribe(status => clientStatuses.push(status));
    client.subscribeToResponseOf('reconnect.sum');
    await client.connect();
  }, 60_000);

  afterAll(async () => {
    await client?.close();
    await app?.close();
  });

  it('should reply again once the broker is back', async () => {
    expect(await sum([1, 2, 3])).toBe('6');

    execFileSync('docker', ['restart', 'test-kafka']);

    // Requests fail while the broker starts up, then the server and the
    // client consume again on their own.
    await vi.waitFor(() => sum([4, 5, 6]).then(r => expect(r).toBe('15')), {
      timeout: 120_000,
      interval: 1_000,
    });
    expect(serverStatuses).toContain(KafkaStatus.CRASHED);
    expect(clientStatuses).toContain(KafkaStatus.CRASHED);
  }, 180_000);
});
