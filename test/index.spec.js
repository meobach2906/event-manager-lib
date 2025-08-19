const amqplib = require('amqplib');

const _is = require("../src/utils/share/_is.utils.share");
const { EventManager, EventManagerFactory, EventTransporter } = require('../index');

const { EventDirectTransporter, EventRabbitMQTransporter } = EventTransporter;

const sleep = (ms) => new Promise((res) => setTimeout(res, ms));


const tests = []

describe('EVENT_SERVICE', () => {

  before(async () => {
    const connection = await amqplib.connect('amqp://meobach2906:random@localhost:5672/event_manager');
    
    await EventDirectTransporter.init();

    EventManager.addListener({ listener: EventDirectTransporter.createListener({
      listener: {
        code: 'DIRECT_LISTENER',
        handler: async ({ data, metadata }) => {
          await sleep(3000);
          console.log('DIRECT_LISTENER', { data, metadata });
        },
      }
    })})

    await EventRabbitMQTransporter.init({ connection });

    EventManager.addListener({ listener: EventRabbitMQTransporter.createListener({
      listener: {
        code: 'MULTI_RABBITMQ_LISTENER',
        handler: async ({ data, metadata }) => {
          await sleep(3000);
          EventManager.throwRetryableError({ reason: 'ABC' });
          console.log('MULTI_RABBITMQ_LISTENER', { data, metadata });
        },
        setting: {
          multi_process: 2,
          multi_worker: {
            worker_number: 5,
            distribute: ({ data }) => data._id % 5
          },
          queue_config: {
            durable: true
          },
          max_retry_times: 3,
          retryable: true,
          ttl_message: 3000,
          fallback_listener: 'DIRECT_LISTENER'
        }
      }
    })})

    EventManager.addListener({ listener: EventRabbitMQTransporter.createListener({
      listener: {
        code: 'RABBITMQ_LISTENER',
        handler: async ({ data, metadata }) => {
          await sleep(3000);
          console.log('RABBITMQ_LISTENER', { data, metadata });
        },
        setting: {
          queue_config: {
            durable: true
          },
          max_retry_times: 3,
          retryable: true,
        }
      }
    })})
    
    EventManager.addEvent({ code: 'EVENT', listener_codes: ['DIRECT_LISTENER', 'RABBITMQ_LISTENER'] });
    EventManager.addListenerToEvent({ event_code: 'EVENT', listener_code: 'MULTI_RABBITMQ_LISTENER' });

    await EventManager.start();
  })

  it('EMIT', async () => {
    await EventManager.emit({ event_code: 'EVENT', data: { _id: 1 } });

    await EventManager.sendToListener({ listener_code: 'RABBITMQ_LISTENER', data: { _id: 2 } });
  })

  it('LISTENER', async () => {
    await EventManager.listen();
  })
})