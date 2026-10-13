import { Controller, Logger } from '@nestjs/common';
import { EventPattern, MessagePattern } from '@nestjs/microservices';
import { BusinessDto } from './dtos/business.dto.js';
import { UserDto } from './dtos/user.dto.js';
import { BusinessEntity } from './entities/business.entity.js';
import { UserEntity } from './entities/user.entity.js';
import { KafkaController } from './kafka.controller.js';

@Controller()
export class KafkaMessagesController {
  protected readonly logger = new Logger(KafkaMessagesController.name);
  static IS_NOTIFIED = false;

  @MessagePattern('math.sum.sync.kafka.message')
  mathSumSyncKafkaMessage(data: any) {
    return (data.numbers || []).reduce((a, b) => a + b);
  }

  @MessagePattern('math.sum.sync.without.key')
  mathSumSyncWithoutKey(data: any) {
    return (data.numbers || []).reduce((a, b) => a + b);
  }

  @MessagePattern('math.sum.sync.plain.object')
  mathSumSyncPlainObject(data: any) {
    return (data.numbers || []).reduce((a, b) => a + b);
  }

  @MessagePattern('math.sum.sync.array')
  mathSumSyncArray(data: any) {
    return (data || []).reduce((a, b) => a + b);
  }

  @MessagePattern('math.sum.sync.string')
  mathSumSyncString(data: any) {
    // this.logger.error(util.format('mathSumSyncString() data: %o', data));
    return (data.split(',') || [])
      .map(i => {
        return parseFloat(i);
      })
      .reduce((a, b) => a + b);
  }

  @MessagePattern('math.sum.sync.number')
  mathSumSyncNumber(data: any) {
    // this.logger.error(util.format('mathSumSyncNumber() data: %o', data));
    return (data.toString().split('') || [])
      .map(i => {
        return parseFloat(i);
      })
      .reduce((a, b) => a + b);
  }

  @MessagePattern(/^math\.sum\.sync\.regex\..+$/)
  mathSumSyncRegex(data: any) {
    return (data.numbers || []).reduce((a, b) => a + b);
  }

  @EventPattern('notify')
  eventHandler(data: any) {
    KafkaController.IS_NOTIFIED = data.notify;
  }

  // Complex data to send.
  @MessagePattern('user.create')
  async createUser(params: { user: UserDto }) {
    return new UserEntity(params.user);
  }

  @MessagePattern('business.create')
  async createBusiness(params: { business: BusinessDto }) {
    return new BusinessEntity(params.business);
  }
}
