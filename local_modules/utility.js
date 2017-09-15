(function moduleUtilityNodejs() {
  /*
    this module exports shared utilities
  */
  'use strict';

  var local = {

    _name: 'utility.moduleCommonNodejs',

    _init: function () {
      EXPORTS.moduleInit = local.moduleInit;
      EXPORTS.moduleInit(typeof module === 'object' ? module : null, local);
    },

    _initOnce: function () {
      /* require */
      required.fs = required.fs || require('fs');
      required.http = required.http || require('http');
      required.http.globalAgent.maxSockets = 256;
      required.https = required.https || require('https');
      required.https.globalAgent.maxSockets = 256;
      required.path = required.path || require('path');
      required.url = required.url || require('url');

      global._ = global.underscore = global.underscore || require('underscore');
      global.moment = global.moment || require('moment-timezone');
      EXPORTS.ASSETS = JSON.parse(required.fs.readFileSync(state.__dirname + '/app/assets.json', 'utf8'));
    },

    moduleInit: function (module, local2) {
      /* assert local2._name */
      console.assert(local2._name, [local2._name]);
      /* exports */
      var name = local2._name.split('.'),
        exports = EXPORTS.required[name[0]] = EXPORTS.required[name[0]] || {};
      Object.keys(local2).forEach(function (key) {
        var match;
        /* dict item */
        if ((match = (/(.+Dict)_(.*)/).exec(key)) !== null) {
          state[match[1]] = state[match[1]] || {};
          state[match[1]][match[2]] = local2[key];
        /* prototype item */
        } else if ((match = (/(.+)_prototype_(.+)/).exec(key)) !== null) {
          local2[match[1]].prototype[match[2]] = local2[key];
        /* export local2 */
        } else if (key[0] === '_') {
          exports[key] = local2[key];
        /* export global */
        } else {
          EXPORTS[key] = local2[key];
        }
      });
      /* first-time init */
      state.initOnceDict = state.initOnceDict || {};
      if (!state.initOnceDict[local2._name]) {
        state.initOnceDict[local2._name] = true;
        /* init once */
        if (local2._initOnce) {
          local2._initOnce();
        }
        /* require once */
        if (module) {
          if (exports.file) {
            return;
          }
          exports.file = (module && module.filename) || 'undefined';
          exports.dir = exports.file.replace((/\/[^\/]+\/*$/), '');
          module.exports = exports;
        }
      }
    },

    clearCallSetInterval: function (key, callback, interval) {
      /*
        this function:
          1. clear interval key
          2. run callback
          3. set interval key to callback
      */
      var dict = state.setIntervalDict = state.setIntervalDict || {};
      if (dict[key]) {
        clearInterval(dict[key]);
      }
      callback();
      return (dict[key] = setInterval(callback, interval));
    },

    urlJoinPathParams: function (path, params, delimiter) {
      var newpath = path || '',
        delimiter = delimiter || '?';
      if (newpath.indexOf(delimiter) < 0) {
        newpath += delimiter;
      }
      Object.keys(params).sort().forEach(function (key, ii) {
        if (typeof params[key] != undefined) {
          if (ii || newpath.slice(-1) !== delimiter) {
            newpath += '&';
          }
          newpath += encodeURIComponent(key) + '=' + encodeURIComponent(params[key]);
        }
      });
      return newpath.replace('?&', '?');
    },

    objectCopyDeep: function (object) {
      /*
        this function returns a deep-copy of an object using JSON.parse(JSON.stringify(object))
      */
      return JSON.parse(JSON.stringify(object));
    },

    jsonParseOrError: function (data) {
      /*
        this function returns JSON.parse(data) or error
      */
      try {
        return JSON.parse(data);
      } catch (error) {
        return error;
      }
    },

    jsonStringifyOrError: function (data) {
      /*
        this function returns JSON.stringify(data) or error
      */
      try {
        return JSON.stringify(data);
      } catch (error) {
        return error;
      }
    },

    isErrorObject: function (object) {
      /*
        this function returns the object if it's an error
      */
      if (Error.prototype.isPrototypeOf(object)) {
        return object;
      }
    },

    createUtcTime: function(str) {
      return new Date(global.moment(new Date(str)).format('YYYY-MM-DDTHH:00')).getTime();
    },

    adjustTimeStrByDay: function(str, day) {
      return moment.utc(local.createUtcTime(str)).add(day, 'days').format('YYYY/MM/DD HH:00');
    },

    assetAddCssJs: function (options) {
      /*
        this function add css and js fields to the data object
      */
      var asset = options.asset,
        req = options.req,
        pageType = options.pageType || '',
        pageConfig = EXPORTS.getChartConf(pageType) || {},
        jsFiles,
        cssFiles,
        data;
      function removePrefix(files) {
        var newfiles = [];
        files.forEach(function(f) {
          newfiles.push(f.replace('app/', '/'));
        });
        return newfiles;
      }

//      if (req.query.debug || state.isDevbox) {
        jsFiles = removePrefix(EXPORTS.ASSETS[asset].js);
        cssFiles = removePrefix(EXPORTS.ASSETS[asset].css);
//      } else {
//        jsFiles = ['/build/js/' + asset + '.min.js'];
//        cssFiles = ['/build/css/' + asset + '.min.css'];
//      }
      data = _.extend({}, {
        logo: '/build/images/' + state.env['logo'],
        css: cssFiles,
        js: jsFiles,
        links: EXPORTS.getConf().links,
        title: pageConfig.title || '',
        desc: pageConfig.desc || '',
        userEmail: req.user && req.user.email || '',
        dimensions: EXPORTS.getChartDimensionsConf(pageType),
        metrics: EXPORTS.getChartMetricsConf(pageType),
        metricsClass: pageConfig.metrics && pageConfig.metrics.class,
        groupbys: EXPORTS.getChartGroupbyConf(pageType),
        groupbysClass: pageConfig.groupbys && pageConfig.groupbys.class,
        settings: JSON.stringify({
          bouncerId: req.user && req.user.email || '',
          pageType: pageType,
          'data-start-time': state.env['data-start-time'],
          'timezone': state.env['timezone']
        })
      }, options.data);
      return data;
    }

  };
  local._init();
}());
