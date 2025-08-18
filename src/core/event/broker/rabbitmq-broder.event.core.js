const _ = require('lodash');

const _do = require("../../../utils/share/_do.utils.share");
const _is = require("../../../utils/share/_is.utils.share");

class EventRabbitMQBroker {

  static broker = null;

  static async init({ connection }) {
    const broker = new EventRabbitMQBroker({ connection });

    broker.channel = await broker.connection.createChannel();

    return broker;
  }
  
  constructor({ connection }) {
    if (!EventRabbitMQBroker.broker) {
      EventRabbitMQBroker.broker = this;
      this.connection = connection;
    }

    return EventRabbitMQBroker.broker;
  }

  channel = null;

  async createListener({ listener }) {
    const config = listener.config || {};

    if (listener.ttl_message != null) {
      _.set(config, 'arguments["x-message-ttl"]', listener.ttl_message);
    }

    return await this.channel.assertQueue(listener.code, config);
  }

  async listener({ listener }) {
    const channel = await this.connection.createChannel();

    const config = listener.config || {};

    if (listener.ttl_message != null) {
      _.set(config, 'arguments["x-message-ttl"]', listener.ttl_message);
    }

    if (!listener.multi_process || listener.multi_process === 1) {
      channel.prefetch(1);
    }

    if (listener.multi_process > 1) {
      channel.prefetch(listener.multi_process);
    }

    await channel.assertQueue(listener.code, config);

    if (listener.multi_worker && listener.multi_worker.worker_number > 0) {
      for (let worker_index = 0; worker_index < listener.multi_worker.worker_number; worker_index++) {
        const channel = await this.connection.createChannel();

        await channel.assertQueue(`${listener.code}-${worker_index}`, config);

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
              const retry_times = Number(_.get(data, 'metadata.retry_times')) || 0;
              _.set(data, 'metadata.retry_times', retry_times + 1)
              if (_is.retry({ error }) && listener.retryable && (listener.max_retry_times === null || retry_times + 1 < listener.max_retry_times)) {
                await this.sendToQueue({ listener_code: listener.code, data: data.data, metadata: data.metadata })
              } else if (listener.alternate_listener) {
                _.set(data, 'metadata.failed_listener', listener.code)
                _.set(data, 'metadata.alternate_listener', listener.alternate_listener)
                await this.sendToQueue({ listener_code: listener.alternate_listener, data: data.data, metadata: data.metadata })
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

            await this.sendToQueue({ listener_code: `${listener.code}-${worker_index}`, data: data.data, metadata: data.metadata })
  
          } else {
            await listener.handler({ data: data.data, metadata: data.metadata });
          }
        } catch (error) {
          const retry_times = Number(_.get(data, 'metadata.retry_times')) || 0;
          _.set(data, 'metadata.retry_times', retry_times + 1)
          if (_is.retry({ error }) && listener.retryable && (listener.max_retry_times === null || retry_times + 1 < listener.max_retry_times)) {
            await this.sendToQueue({ listener_code: listener.code, data: data.data, metadata: data.metadata })
          } else if (listener.alternate_listener) {
            _.set(data, 'metadata.failed_listener', listener.code)
            _.set(data, 'metadata.alternate_listener', listener.alternate_listener)
            await this.sendToQueue({ listener_code: listener.alternate_listener, data: data.data, metadata: data.metadata })
          }
        }

        channel.ack(msg);
      }
    });
  }

  async bindingListener({ event, listeners }) {
    await this.channel.assertExchange(event.code, 'topic', event.config);

    for (const listener of listeners) {
      await this.channel.bindQueue(listener.code, event.code, event.code);
    }
  }

  async emit({ event_code, data }) {
    if (!this.confirm_channel) {
      this.confirm_channel = await this.connection.createConfirmChannel();
    }

    data = {
      data,
      metadata: {
        event_code,
        retry_times: 0,
      },
    };

    return new Promise((res, rej) => this.confirm_channel.publish(event_code, event_code, Buffer.from(JSON.stringify(data)), { persistent: true }, (error) => {
      if (error) {
        return rej(error)
      }
      return res(true)
    }));
  }

  async sendToQueue({ listener_code, data, metadata = {} }) {
    if (!this.confirm_channel) {
      this.confirm_channel = await this.connection.createConfirmChannel();
    }

    data = {
      data,
      metadata: {
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
  EventRabbitMQBroker: EventRabbitMQBroker
}