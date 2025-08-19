const _ = require('lodash');
const EventEmitter = require('events');
const schema_validator = require('schema-validator-lib');

const _CONST = require('../../../utils/share/_CONST.utils.share');
const _ERR = require('../../../utils/share/_ERR.utils.share');

schema_validator.compile({
  code: 'ADD_DIRECT_LISTENER',
  schema: {
    code: { type: 'string', require: true, nullable: false },
    handler: { type: 'async_function', require: true, nullable: true },
  }
})

class EventDirectTransporter {

  static transporter = null;

  CODE = _CONST.EVENT.TRANSPORTER.DIRECT;

  static async init() {
    const transporter = new EventDirectTransporter();

    return transporter;
  }
  
  constructor() {
    if (!EventDirectTransporter.transporter) {
      EventDirectTransporter.transporter = this;
      this.event_emitter = new EventEmitter();
    }

    return EventDirectTransporter.transporter;
  }

  static createListener({ listener = {
    code,
    handler,
  } }) {
    if (!EventDirectTransporter.transporter) {
      throw new Error(`${_CONST.EVENT.TRANSPORTER.DIRECT} transport not be initiated`)
    }
    const { code, handler } = schema_validator.assert_validate({ code: 'ADD_DIRECT_LISTENER', input: listener })
    
    return {
      code,
      transporter: EventDirectTransporter.transporter,
      handler
    };
  }

  async createListener({ listener }) {

  }

  async bindingListener({ event, listener }) {

  }

  async listen({ listener }) {
    this.event_emitter.on(listener.code, (data) => {
      setImmediate(async () => {
        try {
          await listener.handler({ data: data.data, metadata: data.metadata });
        } catch (error) {
          _ERR.log({ error })
        }
      })
    });
  }

  async stopListen({ listener }) {
    this.event_emitter.removeListener(listener.code, listener.handler);
  }

  async emit({ event, data }) {
    const metadata = {
      event_code: event.code,
    };

    for (const listener of event.listeners) {
      if (listener.transporter === this) {
        await this.sendToListener({ listener, data, metadata })
      }
    }
  }

  async sendToListener({ listener, listener_code, data, metadata = {} }) {
    data = {
      data,
      metadata: {
        sendToListener: true,
        ...metadata,
      },
    };

    listener_code = listener ? listener.code : listener_code;

    this.event_emitter.emit(listener_code, data);
  }
}

module.exports = {
  EventDirectTransporter: EventDirectTransporter
}