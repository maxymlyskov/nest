import {
  Consumer,
  Message,
  Producer,
} from '../external/platformatic-kafka.interface.js';
import { BaseRpcContext } from './base-rpc.context.js';

type PlatformaticKafkaContextArgs = [
  message: Message,
  partition: number,
  topic: string,
  headers: Record<string, unknown>,
  consumer: Consumer,
  producer: Producer,
];

/**
 * @publicApi
 */
export class PlatformaticKafkaContext extends BaseRpcContext<PlatformaticKafkaContextArgs> {
  constructor(args: PlatformaticKafkaContextArgs) {
    super(args);
  }

  /**
   * Returns the reference to the original message.
   */
  getMessage() {
    return this.args[0];
  }

  /**
   * Returns the partition.
   */
  getPartition() {
    return this.args[1];
  }

  /**
   * Returns the name of the topic.
   */
  getTopic() {
    return this.args[2];
  }

  /**
   * Returns the message headers, decoded.
   */
  getHeaders() {
    return this.args[3];
  }

  /**
   * Returns the consumer reference.
   */
  getConsumer() {
    return this.args[4];
  }

  /**
   * Returns the producer reference.
   */
  getProducer() {
    return this.args[5];
  }
}
