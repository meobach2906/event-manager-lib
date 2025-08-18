const schema_validator = require('schema-validator-lib');

schema_validator.compile({
  code: 'ADD_LISTENER',
  schema: {
    code: { type: 'string', require: true, nullable: false },
    handler: { type: 'async_function', require: true, nullable: true },
    setting: {
      type: 'object',
      default: {},
      properties: {
        multi_process: { type: 'number', nullable: true, check: { min: 2, max: 5 } },
        multi_worker: { type: 'object', nullable: true, properties: {
          worker_number: { type: 'number', require: true, check: { min: 1, max: 9 } },
          distribute: { type: 'function' },
          keys: { type: 'array', check: { min_length: 1 } }
        }},
        alternate_listener: { type: 'string', nullable: true, check: ({ info: { input, root, field }, value }) => {
          const result = { errors: [] };
          if (value === input.code) {
            result.errors.push({ field, invalid: 'alternate_listener equal listener code' });
          }
          return result;
        }},
        retryable: { type: 'boolean', default: true },
        max_retry_times: { type: 'integer', default: null, nullable: true, check: ({ info: { root, field }, value }) => {
          const result = { errors: [] };
          if (value && !root.retryable) {
            result.errors.push({ field, invalid: 'listener unretryable' });
          }
          return result;
        } },
        ttl_message: { type: 'integer', default: null },
        config: { type: 'object', default: { durable: true }}
      },
    }
  }
})

schema_validator.compile({
  code: 'ADD_EVENT',
  schema: {
    code: { type: 'string', require: true, nullable: false },
    setting: { type: 'object', default: {}, properties: {
      config: { type: 'object', default: { durable: true }}
    }},
    listener_codes: [{ type: 'string', require: true, nullable: false }]
  }
})

const EventManagerFactory = ({ broker }) => {
  const _private = {
    events: {},
    listeners: {},
  };

  const _public = {
    addListener: async function (listener = {
      code,
      setting: {
        multi_process: null, // { process_number: <number>, distribute: <function>, keys: <array> },
        retryable: true,
        max_retry_times: null,
        alternate_listener: null,
        ttl_message: null,
      },
      handler,
    }) {

      if (_private.listeners[listener.code]) {
        throw new Error(`Listener ${listener.code} already exist`);
      }

      const { code, setting, handler } = schema_validator.assert_validate({ code: 'ADD_LISTENER', input: listener })

      if (setting.alternate_listener) {
        if (!_private.listeners[setting.alternate_listener]) {
          throw new Error(`Alternative Listener ${setting.alternate_listener} not exist`);
        }
      }

      _private.listeners[code] = Object.freeze({
        code: code,
        ...setting,
        handler,
      });

      await broker.createListener({ listener: _private.listeners[code] })
    },
    addEvent: async function (event = { code, listener_codes: [] }) {
      if (_private.events[event.code]) {
        throw new Error(`Event ${event.code} already exist`);
      }

      const { code, setting, listener_codes } = schema_validator.assert_validate({ code: 'ADD_EVENT', input: event })


      const listeners = [];

      for (const listener_code of listener_codes) {
        if (!_private.listeners[listener_code]) {
          throw new Error(`Listener ${listener_code} already exist`);
        }
        listeners.push(_private.listeners[listener_code]);
      }

      _private.events[code] = {
        code: code,
        ...setting,
        listener_codes: listener_codes
      };

      await broker.bindingListener({ event, listeners });
    },
    addListenerToEvent: async function ({ event_code, listener_code }) {
      if (!_private.events[event_code]) {
        throw new Error(`Event ${event_code} not exist`);
      }

      if (!_private.listeners[listener_code]) {
        throw new Error(`Listener ${listener_code} not exist`);
      }

      _private.events[event_code].listener_codes.push(listener_code);

      await broker.bindingListener({ event: _private.events[event_code], listeners: _private.listeners[listener_code] });

    },
    emit: async function({ event_code, data }) {
      return broker.emit({ event_code, data })
    },
    sendToQueue: async function({ listener_code, data }) {
      return broker.sendToQueue({ listener_code, data })
    },
    listen: async function () {
      for (const listener_code in _private.listeners) {
        await broker.listener({ listener: _private.listeners[listener_code] });
      }
    }
  };

  return _public;
}

module.exports = {
  EventManagerFactory,
};