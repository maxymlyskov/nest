import {
  ClusterMetadata,
  ExtendedGroupProtocolSubscription,
  GroupPartitionsAssignments,
} from '../../external/platformatic-kafka.interface.js';
import {
  encodeReplyPartitionAssignerUserData,
  platformaticKafkaReplyPartitionAssigner,
} from '../../helpers/platformatic-kafka-reply-partition-assigner.js';

describe('platformatic kafka reply partition assigner', () => {
  const topics = new Set(['topic-A', 'topic-B']);

  const createMetadata = (
    partitionsCounts: Record<string, number>,
  ): ClusterMetadata => ({
    topics: new Map(
      Object.entries(partitionsCounts).map(([topic, partitionsCount]) => [
        topic,
        { partitionsCount },
      ]),
    ),
  });
  const createMembers = (
    previousAssignments: Record<string, Record<string, number> | Buffer>,
  ) =>
    new Map<string, ExtendedGroupProtocolSubscription>(
      Object.entries(previousAssignments).map(([memberId, previous]) => [
        memberId,
        {
          memberId,
          version: 1,
          metadata: Buffer.isBuffer(previous)
            ? previous
            : encodeReplyPartitionAssignerUserData(previous),
        },
      ]),
    );
  const toPlainAssignments = (result: GroupPartitionsAssignments[]) =>
    result.map(({ memberId, assignments }) => ({
      memberId,
      assignments: Object.fromEntries(
        [...assignments.values()].map(({ topic, partitions }) => [
          topic,
          partitions,
        ]),
      ),
    }));

  describe('assign', () => {
    it('should assign all partitions evenly', () => {
      const members = createMembers({
        'member-3': {},
        'member-1': {},
        'member-4': {},
        'member-2': {},
      });
      const metadata = createMetadata({ 'topic-A': 14, 'topic-B': 5 });

      const result = platformaticKafkaReplyPartitionAssigner(
        'member-1',
        members,
        topics,
        metadata,
      );

      expect(toPlainAssignments(result)).toEqual([
        {
          memberId: 'member-1',
          assignments: { 'topic-A': [0, 4, 8, 12], 'topic-B': [0] },
        },
        {
          memberId: 'member-2',
          assignments: { 'topic-A': [1, 5, 9, 13], 'topic-B': [1] },
        },
        {
          memberId: 'member-3',
          assignments: { 'topic-A': [2, 6, 10], 'topic-B': [2, 4] },
        },
        {
          memberId: 'member-4',
          assignments: { 'topic-A': [3, 7, 11], 'topic-B': [3] },
        },
      ]);
    });
  });

  describe('re-assign', () => {
    it('should keep each previous minimum partition', () => {
      const members = createMembers({
        'member-3': { 'topic-A': 0, 'topic-B': 0 },
        'member-1': { 'topic-A': 1, 'topic-B': 1 },
        'member-4': { 'topic-A': 2 },
        'member-2': {},
      });
      const metadata = createMetadata({ 'topic-A': 11, 'topic-B': 7 });

      const result = platformaticKafkaReplyPartitionAssigner(
        'member-1',
        members,
        topics,
        metadata,
      );

      expect(toPlainAssignments(result)).toEqual([
        {
          memberId: 'member-1',
          assignments: { 'topic-A': [1, 4, 8], 'topic-B': [1, 5] },
        },
        {
          memberId: 'member-2',
          assignments: { 'topic-A': [3, 5, 9], 'topic-B': [2, 6] },
        },
        {
          memberId: 'member-3',
          assignments: { 'topic-A': [0, 6, 10], 'topic-B': [0] },
        },
        {
          memberId: 'member-4',
          assignments: { 'topic-A': [2, 7], 'topic-B': [3, 4] },
        },
      ]);
    });

    describe('when two members claim the same previous partition', () => {
      it('should give it to only one of them', () => {
        const members = createMembers({
          'member-1': { 'topic-A': 0 },
          'member-2': { 'topic-A': 0 },
        });
        const metadata = createMetadata({ 'topic-A': 2 });

        const result = platformaticKafkaReplyPartitionAssigner(
          'member-1',
          members,
          new Set(['topic-A']),
          metadata,
        );

        expect(toPlainAssignments(result)).toEqual([
          { memberId: 'member-1', assignments: { 'topic-A': [0] } },
          { memberId: 'member-2', assignments: { 'topic-A': [1] } },
        ]);
      });
    });

    describe('when a previous partition no longer exists', () => {
      it('should assign a partition that does', () => {
        const members = createMembers({ 'member-1': { 'topic-A': 5 } });
        const metadata = createMetadata({ 'topic-A': 2 });

        const result = platformaticKafkaReplyPartitionAssigner(
          'member-1',
          members,
          new Set(['topic-A']),
          metadata,
        );

        expect(toPlainAssignments(result)).toEqual([
          { memberId: 'member-1', assignments: { 'topic-A': [0, 1] } },
        ]);
      });
    });
  });

  describe('when a member sends metadata that is not JSON', () => {
    it('should treat it as having no previous assignment', () => {
      const members = createMembers({
        'member-1': Buffer.from('not-json'),
        'member-2': { 'topic-A': 0 },
      });
      const metadata = createMetadata({ 'topic-A': 2 });

      const result = platformaticKafkaReplyPartitionAssigner(
        'member-1',
        members,
        new Set(['topic-A']),
        metadata,
      );

      expect(toPlainAssignments(result)).toEqual([
        { memberId: 'member-1', assignments: { 'topic-A': [1] } },
        { memberId: 'member-2', assignments: { 'topic-A': [0] } },
      ]);
    });
  });

  describe('when there are more members than partitions', () => {
    it('should leave the extra members without that topic', () => {
      const members = createMembers({ 'member-1': {}, 'member-2': {} });
      const metadata = createMetadata({ 'topic-A': 1 });

      const result = platformaticKafkaReplyPartitionAssigner(
        'member-1',
        members,
        new Set(['topic-A']),
        metadata,
      );

      expect(toPlainAssignments(result)).toEqual([
        { memberId: 'member-1', assignments: { 'topic-A': [0] } },
        { memberId: 'member-2', assignments: {} },
      ]);
    });
  });

  describe('when a topic has no metadata', () => {
    it('should assign none of its partitions', () => {
      const members = createMembers({ 'member-1': {} });
      const metadata = createMetadata({ 'topic-A': 1 });

      const result = platformaticKafkaReplyPartitionAssigner(
        'member-1',
        members,
        topics,
        metadata,
      );

      expect(toPlainAssignments(result)).toEqual([
        { memberId: 'member-1', assignments: { 'topic-A': [0] } },
      ]);
    });
  });

  describe('encodeReplyPartitionAssignerUserData', () => {
    it('should encode the previous assignment as JSON', () => {
      const userData = encodeReplyPartitionAssignerUserData({ 'topic-A': 3 });

      expect(JSON.parse(userData.toString())).toEqual({
        previousAssignment: { 'topic-A': 3 },
      });
    });
  });
});
