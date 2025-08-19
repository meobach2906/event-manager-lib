This is library to manage event

It will emit event through many transporter (Direct (Nodejs EventEmitter), RabbitMQ)

Flow:

```
  1. Init transporter
  2. Create Listener
  3. Create Event
  4. Binding Event with Listener
  5. Start manager
  6. Listen Listener
  7. Emit Event message
```

1. Transporter

  + Transporter: How message transport to Listner

  + List support storage

    + Direct:
      + Use NodeJS EventEmitter
      ```
        const { EventTransporter } = require('event-manager-lib');

        const { EventDirectTransporter, EventRabbitMQTransporter } = EventTransporter;

        await EventDirectTransporter.init();
      ```

    + RabbitMQ
      + Use AMQP to publish and consume message
      ```
        const amqplib = require('amqplib');
        const connection = await amqplib.connect('amqp://<user>:<pass>@localhost:5672/<vhost>');


        await EventRabbitMQTransporter.init({ connection });

      ```

2. TaskManager
  + Manager Event and Listener, use to emit Event.

  ```
    const { EventManager } = require('event-manager-lib');

  ```

  + Add Listener
    + Direct transporter:
    ```
      EventManager.addListener({ listener: EventDirectTransporter.createListener({
        listener: {
          code: 'DIRECT_LISTENER',
          handler: async ({ data, metadata }) => {
          },
        }
      })})
    ```

    + RabbitMQ transporter
    ```
      EventManager.addListener({ listener: EventRabbitMQTransporter.createListener({
        listener: {
          code: 'MULTI_RABBITMQ_LISTENER',
          handler: async ({ data, metadata }) => {
            // EventManager.throwRetryableError({ reason: 'ABC' });
          },
          setting: {
            multi_process: <multi_process>, // prefetch: number unacked message channel consume at same
            multi_worker: { // create multi worker queue
              worker_number: 5, // number of worker queue
              distribute: ({ data }) => data._id % 5 // worker index will receive message
            },
            queue_config: { // queue config
              durable: true
            },
            max_retry_times: 3,
            retryable: true,
            ttl_message: 3000, // message time to live
            fallback_listener: 'DIRECT_LISTENER' // if fail and can not retry => send to fallback listener
          }
        }
      })})
    ```

  + Add Event
  ```
    // add Event
    EventManager.addEvent({ code: 'EVENT', listener_codes: ['DIRECT_LISTENER'] });

    // add Listener to Event
    EventManager.addListenerToEvent({ event_code: 'EVENT', listener_code: 'MULTI_RABBITMQ_LISTENER' });
  ```

  + Start:
    + To binding Event to Listener => can start Emit Event

    ```
      await EventManager.start();
    ```

3. Emit Event

  ```
    await EventManager.emit({ event_code: 'EVENT', data: { _id: 1 } });
  ```

  + Send to specific Listener

  ```
    await EventManager.sendToListener({ listener_code: 'MULTI_RABBITMQ_LISTENER', data: { _id: 2 } });
  ```

4. Listen
  ```
    await EventManager.listen();
  ```
