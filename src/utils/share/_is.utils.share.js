'use strict';

(() => {
  const _is = {};
  const _di = {};

  _is.filled_array = (value) => {
    return Array.isArray(value) && value.length > 0;
  }

  _is.filled_object = (value) => {
    return typeof value === 'object' && value != null && Object.keys(value).length > 0;
  }

  _is.function = (value) => {
    return typeof value === 'function';
  }

  _is.integer = (value) => {
    return typeof value === 'number' && value % 1 === 0;
  }

  _is.retry = ({ error }) => {
    if (error && error.is_retry) {
      return true;
    }
    if(error && error.reactions && _is.filled_array(error.reactions) && error.reactions.includes('RETRY')) {
      return true;
    }
    return false;
  }

  if (module && module.exports) {   
    module.exports = _is;
  } else if (window) {
    _di = window;
    window._is = _is;
  }
})();