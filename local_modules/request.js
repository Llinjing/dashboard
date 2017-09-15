(function moduleRequestNodejs() {
  /*
    this module exports request related methods (http, https)
  */
  'use strict';

  var local = {

    _name: 'request.moduleRequestNodejs',

    _init: function () {
      EXPORTS.moduleInit(typeof module === 'object' ? module : null, local);
    },

    requestNodejs: function (options, callbackFunc) {
//        console.log(['requestNodejs options::::',options]);
      /*
        this convenience function automatically concatenates the response stream
        as utf8 text, and passes the concatenated result to the callback
      */
      var request,
        timeout,
        urlParsed,
        requestModule,
        _onEventError = function (error, data) {
          if (timeout < 0) {
            return;
          }
          clearTimeout(timeout);
          callbackFunc(error, data);
        };

      if (typeof options === 'string') {
        options = { url: options };
      }
      /* default to localhost if missing http://<host> prefix in url */
      if (options.url[0] === '/') {
        options.url = state.localhost + options.url;
      }
      if (options.url.slice(0, 4) !== 'http') {
        options.url = 'http://' + options.url;
      }




      urlParsed = required.url.parse(options.proxy || options.url);
      requestModule = (urlParsed.protocol === 'https:') ? required.https : required.http;

      callbackFunc = callbackFunc || local._onCallbackDefault;

      options.hostname = urlParsed.hostname;
      options.path = options.proxy ? options.url : urlParsed.path;
      options.rejectUnauthorized = false;

      if (options.params) {
        options.path = EXPORTS.urlJoinPathParams(options.path, options.params);
      }
      options.port = urlParsed.port;

      /* set timeout */
      timeout = setTimeout(function () {
        timeout = -1;
        callbackFunc(local._createErrorTimeout());
      }, options.timeout || 60000);

      request = requestModule.request(options, function (response) {
        if (options.onEventResponse && options.onEventResponse(response)) {
          return;
        }
        if (options.redirect !== false) {
          /* http redirect */
          switch (response.statusCode) {
          case 300:
          case 301:
          case 302:
          case 303:
          case 307:
          case 308:
            options.redirected = options.redirected || 0;
            options.redirected += 1;
            if (options.redirected >= 8) {
              _onEventError(new Error('too many http redirects - '
                + response.headers.location));
              return;
            }
            options.url = response.headers.location;
            if (response.statusCode === 303) {
              options.data = null;
              options.method = 'GET';
            }
            EXPORTS.requestNodejs(options, _onEventError);
            return;
          }
        }
        switch (options.dataType) {
        case 'headers':
          _onEventError(null, response.headers);
          return;
        case 'response':
          _onEventError(null, response.on('error', _onEventError));
          return;
        case 'statusCode':
          _onEventError(null, response.statusCode);
          return;
        }


//          console.log(['request---options....',options]);
        local._streamReadOnEventError(response, function (error, data) {
          if (error) {
            _onEventError(error);
            return;
          }
          if (response.statusCode >= 400) {
            _onEventError(new Error((options.method || 'GET') + ' - ' + options.url
              + ' - ' + response.statusCode + ' - ' + data.toString()));
            return;
          }
          switch (options.dataType) {
          case 'binary':
            break;
          /* try to JSON.parse the response */
          case 'json':
            if (EXPORTS.isErrorObject(data = EXPORTS.jsonParseOrError(data))) {
              /* or if parsing fails, pass an error with offending url */
              _onEventError(new Error('invalid json data from ' + options.url));
              return;
            }
            break;
          default:
            data = data.toString();
          }
          _onEventError(null, data);
        });
      }).on('error', _onEventError);

      if (options.file) {
        options.readStream = options.readStream || required.fs.createReadStream(options.file);
      }

      if (options.readStream) {
        options.readStream.on('error', _onEventError).pipe(request.on('error', _onEventError));
      } else {
        request.end(options.data);
      }
    },

    _onCallbackDefault: function (error, data) {
      /*
        this function provides a common, default error / data callback.
        on error, it will print the error statck.
        on data, it will print the data.
      */
      if (error) {
        /* debug */
        state.error = error;
        console.error(error.stack || error.message || error);
        return;
      }
      if (data === undefined) {
        return;
      }
      console.log((global.Buffer && global.Buffer.isBuffer(data)) ? data.toString() : data);
    },

    _createErrorTimeout: function (message) {
      /*
        this function creates a new timeout error
      */
      var error = new Error(message);
      error.code = error.errno = 'ETIMEDOUT';
      return error;
    },

    _streamReadOnEventError: function (readable, callback) {
      /*
        this function concats data from readable stream and passes it to callback when done
      */
      var chunks = [];
      readable.on('data', function (chunk) {
        chunks.push(chunk);
      }).on('error', callback).on('end', function () {
        callback(null, global.Buffer.concat(chunks));
      });
    }

  };

  local._init();
}());
