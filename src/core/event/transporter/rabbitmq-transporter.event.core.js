const _ = require('lodash');
const schema_validator = require('schema-validator-lib');

const _do = require("../../../utils/share/_do.utils.share");
const _is = require("../../../utils/share/_is.utils.share");
const _CONST = require('../../../utils/share/_CONST.utils.share');
const _ERR = require('../../../utils/share/_ERR.utils.share');

schema_validator.compile({
  code: 'ADD_RABBITMQ_LISTENER',
  schema: {
    code: { type: 'string', require: true, nullable: false },
    handler: { type: 'async_function', require: true, nullable: true },
    setting: {
      type: 'object',
      default: {},
      properties: {
        multi_process: { type: 'number', default: 1, check: { min: 1, max: 5 } },
        multi_worker: { type: 'object', nullable: true, properties: {
          worker_number: { type: 'number', require: true, check: { min: 1, max: 9 } },
          distribute: { type: 'function', require: false },
          keys: { type: 'array', element: { type: 'string' }, require: false, check: { min_length: 1 } }
        }},
        fallback_listener: { type: 'string', nullable: true, check: ({ info: { input, root, field }, value }) => {
          const result = { errors: [] };
          if (value === input.code) {
            result.errors.push({ field, invalid: 'fallback_listener equal listener code' });
          }
          return result;
        }},
        retryable: { type: 'boolean', default: true },
        max_retry_times: { type: 'integer', nullable: true, check: ({ info: { root, field }, value }) => {
          const result = { errors: [] };
          if (value && !root.retryable) {
            result.errors.push({ field, invalid: 'listener unretryable' });
          }
          return result;
        } },
        ttl_message: { type: 'integer', nullable: true },
        queue_config: { type: 'object', default: { durable: true }, properties: {}}
      },
    }
  }
})

class EventRabbitMQTransporter {

  static transporter = null;

  CODE = _CONST.EVENT.TRANSPORTER.RABBITMQ;

  static async init({ connection }) {
    const transporter = new EventRabbitMQTransporter({ connection });

    transporter.channel = await transporter.connection.createChannel();

    return transporter;
  }
  
  constructor({ connection }) {
    if (!EventRabbitMQTransporter.transporter) {
      EventRabbitMQTransporter.transporter = this;
      this.connection = connection;
    }

    return EventRabbitMQTransporter.transporter;
  }

  static createListener({ listener = {
    code,
    setting: {
      multi_process: 1,
      multi_worker: {
        worker_number,
        distribute,
        keys,
      },
      fallback_listener: null,
      retryable: true,
      max_retry_times: null,
      ttl_message: null,
      queue_config: {
        durable: true
      }
    },
    handler,
  } }) {
    if (!EventRabbitMQTransporter.transporter) {
      throw new Error(`${_CONST.EVENT.TRANSPORTER.RABBITMQ} transport not be initiated`)
    }
    const { code, setting, handler } = schema_validator.assert_validate({ code: 'ADD_RABBITMQ_LISTENER', input: listener })
    
    return {
      code,
      transporter: EventRabbitMQTransporter.transporter,
      ...setting, 
      handler
    };
  }

  channel = null;

  async createListener({ listener }) {
    const queue_config = listener.queue_config || {};

    if (listener.ttl_message != null) {
      _.set(queue_config, 'arguments["x-message-ttl"]', listener.ttl_message);
    }

    return await this.channel.assertQueue(listener.code, queue_config);
  }

  async bindingListener({ event, listener }) {
    await this.channel.assertExchange(event.code, 'topic', { durable: true });

    await this.channel.bindQueue(listener.code, event.code, event.code);
  }

  async listen({ listener, event_manager }) {
    const result = {
      channels: [],
    };

    const channel = await this.connection.createChannel();

    result.channels.push(channel);

    const queue_config = listener.queue_config || {};

    if (listener.ttl_message != null) {
      _.set(queue_config, 'arguments["x-message-ttl"]', listener.ttl_message);
    }

    if (!listener.multi_process || listener.multi_process === 1) {
      channel.prefetch(1);
    }

    if (listener.multi_process > 1) {
      channel.prefetch(listener.multi_process);
    }

    await channel.assertQueue(listener.code, queue_config);

    if (listener.multi_worker && listener.multi_worker.worker_number > 0) {
      for (let worker_index = 0; worker_index < listener.multi_worker.worker_number; worker_index++) {
        const channel = await this.connection.createChannel();
        result.channels.push(channel);

        await channel.assertQueue(`${listener.code}-${worker_index}`, queue_config);

        if (!listener.multi_process || listener.multi_process === 1) {
          channel.prefetch(1);
        }

        if (listener.multi_process > 1) {
          channel.prefetch(listener.multi_process);
        }

        channel.consume(`${listener.code}-${worker_index}`, async (msg) => {
          if (msg !== null) {
            const data = JSON.parse(msg.content.toString());

            try {
              await listener.handler({ data: data.data, metadata: data.metadata });
            } catch (error) {
              _ERR.log({ error })
              const retry_times = Number(_.get(data, 'metadata.retry_times')) || 0;
              _.set(data, 'metadata.retry_times', retry_times + 1)
              if (_is.retry({ error }) && listener.retryable && (listener.max_retry_times === null || retry_times + 1 < listener.max_retry_times)) {
                await this.sendToListener({ listener_code: listener.code, data: data.data, metadata: data.metadata })
              } else if (listener.fallback_listener) {
                _.set(data, 'metadata.failed_listener', listener.code)
                _.set(data, 'metadata.fallback_listener', listener.fallback_listener)
                await event_manager.sendToListener({ listener_code: listener.fallback_listener, data: data.data, metadata: data.metadata })
              }
            }
    
            channel.ack(msg);
          }
        });
      }
    }

    channel.consume(listener.code, async (msg) => {
      if (msg !== null) {

        const data = JSON.parse(msg.content.toString());

        try {

          if (listener.multi_worker && listener.multi_worker.worker_number > 0) {
            let worker_index = null;
            if (listener.multi_worker.distribute && _is.function(listener.multi_worker.distribute)) {
              worker_index = listener.multi_worker.distribute({ data: data.data, metadata: data.metadata });
            } else {
              const keys = _is.filled_array(listener.multi_worker.keys) ? listener.multi_worker.keys : listener.multi_worker.keys.split(',');
  
              const value = keys.map(key => data.data[key] || '').join('-');
  
              worker_index = _do.hash_to_number(value) % listener.multi_worker.worker_number;
            }
  
            if (!_is.integer(worker_index) || worker_index >= listener.multi_worker.worker_number || worker_index < 0) {
              throw new Error(`Invalid worker_index`);
            }

            if (!this.confirm_channel) {
              this.confirm_channel = await this.connection.createConfirmChannel();
            }

            data.metadata = data.metadata || {};

            data.metadata.worker_index = worker_index;

            await this.sendToListener({ listener_code: `${listener.code}-${worker_index}`, data: data.data, metadata: data.metadata })
  
          } else {
            await listener.handler({ data: data.data, metadata: data.metadata });
          }
        } catch (error) {
          _ERR.log({ error })
          const retry_times = Number(_.get(data, 'metadata.retry_times')) || 0;
          _.set(data, 'metadata.retry_times', retry_times + 1)
          if (_is.retry({ error }) && listener.retryable && (listener.max_retry_times === null || retry_times + 1 < listener.max_retry_times)) {
            await this.sendToListener({ listener_code: listener.code, data: data.data, metadata: data.metadata })
          } else if (listener.fallback_listener) {
            _.set(data, 'metadata.failed_listener', listener.code)
            _.set(data, 'metadata.fallback_listener', listener.fallback_listener)
            await this.sendToListener({ listener_code: listener.fallback_listener, data: data.data, metadata: data.metadata })
          }
        }

        channel.ack(msg);
      }
    });

    return result;
  }

  async stopListen({ listener }) {
    if (_is.filled_array(listener.channels)) {
      for (const channel of listener.channels) {
        await channel.close();
      }
    }
  }

  async emit({ event, data }) {
    if (!this.confirm_channel) {
      this.confirm_channel = await this.connection.createConfirmChannel();
    }

    data = {
      data,
      metadata: {
        event_code: event.code,
        retry_times: 0,
      },
    };

    return new Promise((res, rej) => this.confirm_channel.publish(event.code, event.code, Buffer.from(JSON.stringify(data)), { persistent: true }, (error) => {
      if (error) {
        return rej(error)
      }
      return res(true)
    }));
  }

  async sendToListener({ listener, listener_code, data, metadata = {} }) {
    if (!this.confirm_channel) {
      this.confirm_channel = await this.connection.createConfirmChannel();
    }

    listener_code = listener ? listener.code : listener_code;

    data = {
      data,
      metadata: {
        sendToListener: true,
        retry_times: 0,
        ...metadata,
      },
    };

    return new Promise((res, rej) => this.confirm_channel.sendToQueue(listener_code, Buffer.from(JSON.stringify(data)), { persistent: true }, (error) => {
      if (error) {
        return rej(error)
      }
      return res(true)
    }));
  }
}

module.exports = {
  EventRabbitMQTransporter: EventRabbitMQTransporter
}