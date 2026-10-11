/**
 * Do NOT add NestJS logic to this interface. It is meant to ONLY represent the
 * types of the @platformatic/kafka package that the transport uses.
 *
 * @see https://github.com/platformatic/kafka/tree/main/src/clients
 *
 * @publicApi
 */

/// <reference types="node" />
import { ConnectionOptions as TLSConnectionOptions } from 'tls';

export interface Broker {
  host: string;
  port: number;
}

export interface SASLOptions {
  mechanism:
    'PLAIN' | 'SCRAM-SHA-256' | 'SCRAM-SHA-512' | 'OAUTHBEARER' | 'GSSAPI';
  username?: string;
  password?: string;
  token?: string;
}

export interface ConnectionOptions {
  connectTimeout?: number;
  requestTimeout?: number;
  maxInflights?: number;
  tls?: TLSConnectionOptions;
  tlsServerName?: string | boolean;
  sasl?: SASLOptions;
}

export interface BaseOptions extends ConnectionOptions {
  clientId: string;
  bootstrapBrokers: Broker[] | string[];
  clientRack?: string;
  timeout?: number;
  retries?: number | boolean;
  retryDelay?: number;
  metadataMaxAge?: number;
  autocreateTopics?: boolean;
  strict?: boolean;
}

export interface GroupAssignment {
  topic: string;
  partitions: number[];
}

export interface GroupOptions {
  sessionTimeout?: number;
  rebalanceTimeout?: number;
  heartbeatInterval?: number;
  groupInstanceId?: string;
}

export interface ConsumeBaseOptions {
  autocommit?: boolean | number;
  minBytes?: number;
  maxBytes?: number;
  maxWaitTime?: number;
  isolationLevel?: number;
  highWaterMark?: number;
}

export interface StreamOptions {
  topics: string[];
  mode?: 'latest' | 'earliest' | 'committed' | 'manual';
  fallbackMode?: 'latest' | 'earliest' | 'fail';
  maxFetches?: number;
}

export type ConsumeOptions = StreamOptions & ConsumeBaseOptions & GroupOptions;

export type ConsumerOptions = BaseOptions & {
  groupId: string;
} & GroupOptions &
  ConsumeBaseOptions;

export interface ProduceOptions {
  idempotent?: boolean;
  acks?: number;
  compression?: 'none' | 'gzip' | 'snappy' | 'lz4' | 'zstd';
  autocreateTopics?: boolean;
}

export type ProducerOptions = BaseOptions & ProduceOptions;

export interface MessageToProduce {
  topic: string;
  key?: Buffer;
  value?: Buffer;
  partition?: number;
  headers?: Map<Buffer, Buffer>;
}

export type SendOptions = { messages: MessageToProduce[] } & ProduceOptions;

export interface TopicWithPartitionAndOffset {
  topic: string;
  partition: number;
  offset: bigint;
}

export interface ProduceResult {
  offsets?: TopicWithPartitionAndOffset[];
  unwritableNodes?: number[];
}

export interface Message {
  topic: string;
  partition: number;
  key: Buffer;
  value: Buffer;
  headers: Map<Buffer, Buffer>;
  offset: bigint;
  timestamp: bigint;
  commit(): void | Promise<void>;
}

export interface ConsumerGroupJoinPayload {
  groupId: string;
  memberId: string;
  generationId?: number;
  isLeader?: boolean;
  assignments?: GroupAssignment[];
}

export interface ClientEvents {
  'client:broker:connect': () => void;
  'client:broker:disconnect': () => void;
  'client:broker:failed': () => void;
}

export interface ConsumerEvents extends ClientEvents {
  'consumer:group:join': (payload: ConsumerGroupJoinPayload) => void;
  'consumer:group:rebalance': () => void;
}

export interface MessagesStream extends AsyncIterable<Message> {
  close(): Promise<void>;
}

export interface Consumer {
  groupId: string;
  assignments: GroupAssignment[] | null;
  on<E extends keyof ConsumerEvents>(
    event: E,
    listener: ConsumerEvents[E],
  ): this;
  consume(options: ConsumeOptions): Promise<MessagesStream>;
  close(force?: boolean): Promise<void>;
}

export interface Producer {
  on<E extends keyof ClientEvents>(event: E, listener: ClientEvents[E]): this;
  send(options: SendOptions): Promise<ProduceResult>;
  close(force?: boolean): Promise<void>;
}

export interface PlatformaticKafkaPackage {
  Consumer: new (options: ConsumerOptions) => Consumer;
  Producer: new (options: ProducerOptions) => Producer;
}
