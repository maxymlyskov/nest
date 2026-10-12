import {
  ExtendedGroupProtocolSubscription,
  GroupAssignment,
  GroupPartitionsAssigner,
  GroupProtocolSubscription,
} from '../external/platformatic-kafka.interface.js';
import { isNumber, isObject } from '@nestjs/common/internal';

type MinimumPartitions = Record<string, number>;

/**
 * The group protocol advertised by `ClientPlatformaticKafka`. Its metadata
 * carries each member's previous minimum partition per reply topic.
 */
export const PLATFORMATIC_KAFKA_REPLY_PROTOCOL: GroupProtocolSubscription = {
  name: 'NestReplyPartitionAssigner',
  version: 1,
};

export function encodeReplyPartitionAssignerUserData(
  previousAssignment: MinimumPartitions,
): Buffer {
  return Buffer.from(JSON.stringify({ previousAssignment }));
}

function decodePreviousAssignment(
  member: ExtendedGroupProtocolSubscription,
): MinimumPartitions {
  if (!member.metadata || member.metadata.length === 0) {
    return {};
  }
  try {
    const userData = JSON.parse(member.metadata.toString());
    return isObject(userData?.previousAssignment)
      ? userData.previousAssignment
      : {};
  } catch {
    return {};
  }
}

/**
 * The `KafkaReplyPartitionAssigner` algorithm for `@platformatic/kafka`: every
 * member gets at least one partition of each reply topic and keeps its previous
 * minimum partition, since that is the one the server replies to.
 */
export const platformaticKafkaReplyPartitionAssigner: GroupPartitionsAssigner =
  (current, members, topics, metadata) => {
    const memberIds = [...members.keys()].sort();
    if (memberIds.length === 0) {
      return [];
    }
    const freePartitions = new Map<string, number[]>();
    for (const topic of topics) {
      const partitionsCount = metadata.topics.get(topic)?.partitionsCount ?? 0;
      freePartitions.set(
        topic,
        Array.from({ length: partitionsCount }, (_, partition) => partition),
      );
    }

    const assignment = new Map<string, Map<string, number[]>>();
    for (const memberId of memberIds) {
      const previousAssignment = decodePreviousAssignment(
        members.get(memberId)!,
      );
      const memberAssignment = new Map<string, number[]>();
      for (const [topic, free] of freePartitions) {
        const previousPartition = previousAssignment[topic];
        const index = isNumber(previousPartition)
          ? free.indexOf(previousPartition)
          : -1;
        // A partition another member already kept is not handed out twice.
        memberAssignment.set(topic, index === -1 ? [] : free.splice(index, 1));
      }
      assignment.set(memberId, memberAssignment);
    }

    for (const memberId of memberIds) {
      for (const [topic, partitions] of assignment.get(memberId)!) {
        const free = freePartitions.get(topic)!;
        if (partitions.length === 0 && free.length > 0) {
          partitions.push(free.shift()!);
        }
      }
    }

    let index = 0;
    for (const [topic, free] of freePartitions) {
      for (const partition of free) {
        const memberId = memberIds[index++ % memberIds.length];
        assignment.get(memberId)!.get(topic)!.push(partition);
      }
    }

    return memberIds.map(memberId => {
      const assignments = new Map<string, GroupAssignment>();
      for (const [topic, partitions] of assignment.get(memberId)!) {
        if (partitions.length > 0) {
          assignments.set(topic, { topic, partitions });
        }
      }
      return { memberId, assignments };
    });
  };
