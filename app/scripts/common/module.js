window.global = window;
(function(global) {
  var mapping = {},
    cached = {};
  global.define = function(id, func) {
    if(mapping[id]) {
      throw new Error('[ "' + id + '" ] already exists, don\'t repeat the definition');
    }
    mapping[id] = func;
  };
  global.require = function(id) {
    if(cached[id]) {
      return cached[id];
    } else {
      cached[id] = {};
      mapping[id](cached[id]);
      return cached[id];
    }
  };
})(this);
