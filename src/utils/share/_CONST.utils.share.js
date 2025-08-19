'use strict';

(() => {

  const _CONST = {
    EVENT: {
      TRANSPORTER: {
        RABBITMQ: 'RABBITMQ',
        SOCKETIO: 'SOCKETIO',
        DIRECT: 'DIRECT',
      }
    }
  };

  if (module && module.exports) {
    module.exports = _CONST;
  } else if (window) {
    _di = window;
    window._CONST = _CONST;
  }
})();