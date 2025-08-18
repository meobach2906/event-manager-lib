'use strict';

(() => {
  const _do = {};
  const _di = {};

  _do.hash_to_number = (str) => {
    let hash = 0;

    for (let i = 0; i < str.length; i++) {
      hash = (hash << 5) - hash + str.charCodeAt(i); // hash * 31 + char
      hash |= 0; // Force to 32-bit integer
    }
  
    return Math.abs(hash);
  }

  if (module && module.exports) {   
    module.exports = _do;
  } else if (window) {
    _di = window;
    window._do = _do;
  }
})();