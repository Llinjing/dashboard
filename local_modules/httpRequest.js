(function moduleHttpRequestNodejs() {
  /*
   this module exports druid related methods
   */
  'use strict';

  var local = {

    _name: 'request.moduleHttpRequestNodejs',

    _init: function () {
      EXPORTS.moduleInit(typeof module === 'object' ? module : null, local);
    },

    //考虑到洪安那里的接口。不能处理有计算的字段，因此，metrics，现在先具体指定
    //_getMetrics: function(options) {
    //  var metricsArray = EXPORTS.getChartMetricsConf(options.dataType, 'mysql') || [],
    //      groupbys = EXPORTS.getChartGroupbyConf(options.dataType) || [],
    //      chartType = EXPORTS.getChartConfType(options.dataType) || [],
    //      metricsStrArr = [];
    //  console.log("metricsArray " + JSON.stringify(metricsArray));
	//
	//
    //  if (metricsArray.length === 1) {
    //    return 'Value';
    //  } else {
    //    metricsArray.forEach(function(m) {
    //      var mname = (options.groupBy||(groupbys.length != 0&&chartType != 'table_only')) && 'SUM('+m.name+') AS '+m.name || m.name;
    //      metricsStrArr.push(mname);
    //    });
    //  }
    //  return metricsStrArr.join(', ');
    //},

    constructhttpRequestBody: function(options,metrics,dataSource) {

      var request = require("request"),
          param = {},
          res,
          url,
          postBody,
          dime = {},
          groupby = options.groupBy;

      param["start_time"] = moment(new Date(options.startTime)).format('YYYY-MM-DD HH:mm:ss');
      param["end_time"] = moment(new Date(options.endTime)).format('YYYY-MM-DD HH:mm:ss');

      for(var key in options.filter) {
        dime[key] = options.filter[key].values;
      }

      param["dimensions"] = dime;
      param["groupby"] = groupby;
      param["metrics"] = metrics;

      console.log("param is "+JSON.stringify(param));

      //res = Buffer.from(param, "utf8").toString("base64");
      res = new Buffer(JSON.stringify(param)).toString("base64");
      url = "http://10.10.200.66:8989/history?session_id="+options["session_id"]+"&data_source="+dataSource+"&query_info="+res;

      console.log("request url is "+url);

      postBody = {
        'url':url
      }

      return postBody;
    },

    requestHttpData: function(data, callback) {
      var getBody = JSON.stringify(data, null, 4);
      EXPORTS.requestNodejs({
        url:data.url,
        method: 'GET',
        data: getBody
      }, function(err, res) {
        var data;

        if (err) {
          console.error(err);
        }

        try {
          data = JSON.parse(res);
        } catch (e) {
          callback(e);
          return;
        }
        callback(err, data);
      });
    }
  };

  local._init();
}());
