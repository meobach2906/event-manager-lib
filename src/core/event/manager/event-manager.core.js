const schema_validator = require('schema-validator-lib');

const _CONST = require('../../../utils/share/_CONST.utils.share');
const _ERR = require('../../../utils/share/_ERR.utils.share');

schema_validator.compile({
  code: 'ADD_LISTENER',
  schema: {
    code: { type: 'string', require: true, nullable: false },
    transporter: { type: 'object', require: true, nullable: false, properties: {
      CODE: { type: 'string', require: true, nullable: false, enum: Object.values(_CONST.EVENT.TRANSPORTER) }
    }},
    handler: { type: 'async_function', require: true, nullable: true },
  }
})

schema_validator.compile({
  code: 'ADD_EVENT',
  schema: {
    code: { type: 'string', require: true, nullable: false },
    setting: { type: 'object', default: {}, properties: {
      default_exchange_config: { type: 'object', default: { durable: true }, properties: {}}
    }},
    listener_codes: [{ type: 'string', require: true, nullable: false }]
  }
})

const EventManagerFactory = () => {
  const _private = {
    events: {},
    listeners: {},
  };

  const _public = {
    addListener: function ({ listener }) {

      listener = schema_validator.assert_validate({ code: 'ADD_LISTENER', input: listener });

      if (_private.listeners[listener.code]) {
        throw new Error(`Listener ${listener.code} already exist`);
      }

      if (listener.transporter === _CONST.EVENT.TRANSPORTER.RABBITMQ && listener.fallback_listener) {
        if (!_private.listeners[listener.fallback_listener]) {
          throw new Error(`Fallback Listener ${setting.fallback_listener} not exist`);
        }
      }

      _private.listeners[listener.code] = listener;
    },
    addEvent: function (event = { code, listener_codes: [] }) {
      if (_private.events[event.code]) {
        throw new Error(`Event ${event.code} already exist`);
      }

      const { code, listener_codes } = schema_validator.assert_validate({ code: 'ADD_EVENT', input: event })


      const listeners = [];
      const transporters = [];

      for (const listener_code of listener_codes) {
        const listener = _private.listeners[listener_code];
        if (!listener) {
          throw new Error(`Listener ${listener_code} already exist`);
        }
        listeners.push(listener);
        if (!transporters.find(transporter => transporter.CODE === listener.transporter.CODE)) {
          transporters.push(listener.transporter);
        }
      }

      _private.events[code] = {
        code: code,
        listener_codes: listener_codes,
        listeners: listeners,
        transporters,
      };

    },
    addListenerToEvent: function ({ event_code, listener_code }) {
      if (!_private.events[event_code]) {
        throw new Error(`Event ${event_code} not exist`);
      }

      if (!_private.listeners[listener_code]) {
        throw new Error(`Listener ${listener_code} not exist`);
      }

      _private.events[event_code].listener_codes.push(listener_code);
      _private.events[event_code].listeners.push(_private.listeners[listener_code]);
    },
    start: async function() {
      for (const listener of Object.values(_private.listeners)) {
        await listener.transporter.createListener({ listener });
      }

      for (const event of Object.values(_private.events)) {
        for (const listener of event.listeners) {
          await listener.transporter.bindingListener({ event, listener });
        }
      }
    },
    emit: async function({ event_code, data }) {
      const event = _private.events[event_code];
      if (!event) {
        throw new Error(`Event ${event_code} not found`);
      }
      for (const transporter of event.transporters) {
        await transporter.emit({ event, data })
      }
    },
    sendToListener: async function({ listener_code, data, metadata = {} }) {
      const listener = _private.listeners[listener_code];
      if (!listener) {
        throw new Error(`Listener ${listener_code} not found`);
      }
      return listener.transporter.sendToListener({ listener, data, metadata })
    },
    listen: async function () {
      for (const listener of Object.values(_private.listeners)) {
        const result = await listener.transporter.listen({ listener: listener, event_manager: _public });
        if (listener.transporter.CODE === _CONST.EVENT.TRANSPORTER.RABBITMQ) {
          listener.channels = result.channels;
        }
      }
    },
    stopListen: async function({ listener_code }) {
      const listener = _private.listeners[listener_code];
      if (!listener) {
        throw new Error(`Listener ${listener_code} not found`);
      }
      await listener.transporter.stopListen({ listener: listener });
    },
    stopListenAll: async function() {
      for (const listener of Object.values(_private.listeners)) {
        await listener.transporter.stopListen({ listener: listener });
      }
    },
    throwError: (reason) => {
      throw new _ERR.ERR(reason);
    },
    throwRetryableError: (reason) => {
      throw new _ERR.TEMPORARILY_ERR(reason);
    },
  };

  return _public;
}

module.exports = {
  EventManagerFactory,
};