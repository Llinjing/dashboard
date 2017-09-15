var express = require('express');
var router = express.Router();
var async = require('async');
var app = express();

router.post('/data/chart_and_table/:chart', function(req, res) {

  var query = req.body || {},
    isGetCompared = req.query.type === 'compare',
    filter = query.filter || {},
    combinationFilterList,
    chart = req.params.chart,
    config = EXPORTS.getChartConf(chart),
    dataSource = EXPORTS.getChartDataSourceConf(chart),
    asyncFunc = {};



    //有filterMetric 的多次查询
function queryMulFilterr(){
    if(dataSource.druid.filter_metrics){
        var ext_filter  =[];
        var ext_metrics = [];
        dataSource.druid.filter_metrics.forEach(function(e) {
            ext_metrics.push( e.keys.split(","));
            ext_filter.push(e.filter);
        }) ;
    }
}

  function parseAndCalculateRes(resList, err, callback) {

    if (err) {
      console.error(err)
      callback(err);
      return;
    }

    var derivedRes = calcDerivedMetrics(resList, chart),
      pointCount = derivedRes.length,
      sumRes = {},
      idNameMap = EXPORTS.getChartMetricsIdNameMap(chart),
      metricIDs = _.keys(idNameMap);

    metricIDs.forEach(function(mid) { 
      mname = idNameMap[mid].name;
      sumRes[mname] = 0; 
    });
    // compute sum
    derivedRes.forEach(function(point){

      var values = point.result;
      metricIDs.forEach(function(mid){
        mname = idNameMap[mid].name;
        values[mname] = values[mname] || 0;
        sumRes[mname] += (values[mname] / pointCount);
      });
    });

    metricIDs.forEach(function(mid) {
      mname = idNameMap[mid].name;
      sumRes[mname] = sumRes[mname]>1000 && Math.round(sumRes[mname]) || sumRes[mname];
    });
    callback(null, {
      data: derivedRes,
      sum: sumRes
    });
  }

  function calcDerivedMetrics(resList, chartName) {
    // Use the first result set on the list as the base to get timestamps
    var res0 = resList && resList[0] || [],
      resLen = resList.length,
      resMap = {},
      i;
    var calculateMetrics = EXPORTS.getChartMetricsNeedCalculate(chartName),
      idNameMap = EXPORTS.getChartMetricsIdNameMap(chartName);

    // make a resMap[res-i][time] -> a list of metric values
    for (i = 0; i < resLen; i++) {
      resMap['res' + i] = {};
      (resList[i] || []).forEach(function(r) {
        resMap['res' + i][new Date(r.timestamp).getTime()] = r.result;
      });
    }

    res0.forEach(function(item) {
      var time = new Date(item.timestamp).getTime(),
        allMetrics = item && item.result || {};

      for (i = 1; i < resLen; i++) {
        _.extend(allMetrics, resMap['res' + i][time]);
      }

      calculateMetrics.forEach(function(cm) {
        var evalState = '',
          dependency = cm.dependency || [],
          mid,
          mname,
          j;

        for (j = 0; j < dependency.length; j++) {
          mid = dependency[j]; // metrics id
          mname = idNameMap[mid].name; // metrics name (field names in druid or MySQL)
          evalState += ('var ' + mid + ' = ' + allMetrics[mname] + ';');
        }
        evalState += 'expressR = ' + cm.expression + ';';

        try {
          eval(evalState);
          if (!isNaN(expressR)) {
            allMetrics[cm.name] = expressR;
          }else {
            allMetrics[cm.name] = 0;
          }
        }
        catch(exception) {
          allMetrics[cm.name] = 0;
        }

      });

    });
    return res0;
  }

    //多次查询信息
   function mulDruidRequest(q,callback){
       for(i in dataSource.length){
           EXPORTS.requestDruidData(EXPORTS.constructDruidPostBody(q), function(err, res) {
               parseAndCalculateRes([res], err, callback);
           });
       }
    }

  function requestData(q, callback) {
 if(dataSource.mysql_ad_news){
      EXPORTS.requestMysqlData(EXPORTS.constructADNewsIndicator(q),function(err, res) {
        res.forEach(function(data){
          timezone = moment().tz("Asia/Shanghai").format('Z');
          data["timestamp"]  = moment(data["result"]["timestamp"]).format('YYYY-MM-DDTHH:mm') + timezone;
          delete data["result"]["timestamp"];
        });
        parseAndCalculateRes([res], err, callback);
      }, req);
 }else if(dataSource.mysql_ad_report){
      EXPORTS.requestMysqlData(EXPORTS.construcADReportServer(q),function(err, res) {
        res.forEach(function(data){
          timezone = moment().tz("Asia/Shanghai").format('Z');
          data["timestamp"]  = moment(data["result"]["timestamp"]).format('YYYY-MM-DDTHH:mm') + timezone;
          delete data["result"]["timestamp"];
        });
        parseAndCalculateRes([res], err, callback);
      }, req);
 }else if(dataSource.mysql_explore){
   EXPORTS.requestMysqlData(EXPORTS.constructExplore(q),function(err, res) {
     res.forEach(function(data){
       timezone = moment().tz("Asia/Shanghai").format('Z');
       data["timestamp"]  = moment(data["result"]["timestamp"]).format('YYYY-MM-DDTHH:mm') + timezone;
       delete data["result"]["timestamp"];
     });
     parseAndCalculateRes([res], err, callback);
   }, req);
 }else if(dataSource.mysql_ad_chart){
   EXPORTS.requestMysqlData(EXPORTS.constructADDailyChart(q),function(err, res) {
     res.forEach(function(data){
       timezone = moment().tz("Asia/Shanghai").format('Z');
       data["timestamp"]  = moment(data["result"]["timestamp"]).format('YYYY-MM-DDTHH:mm') + timezone;
       delete data["result"]["timestamp"];
     });
     parseAndCalculateRes([res], err, callback);
   }, req);
 }else if(dataSource.mysql_push12hours){
      EXPORTS.requestMysqlData(EXPORTS.constructPush12hours(q),function(err, res) {
        res.forEach(function(data){
          timezone = moment().tz("Asia/Shanghai").format('Z');
          data["timestamp"]  = moment(data["result"]["timestamp"]).format('YYYY-MM-DDTHH:mm') + timezone;
          delete data["result"]["timestamp"];
          data['result']['content_id'] = '<a href="http://52.220.20.252/newsContent/index?contentId='+data['result']['content_id'] +'" target="_blank" title="'+data['result']['content_id']+'">'+data['result']['content_id'] +'</a>';
        });
        parseAndCalculateRes([res], err, callback);
      }, req);
 }else if(dataSource.mysql_sharenum){
      EXPORTS.requestMysqlData(EXPORTS.constructSharearticlenum(q),function(err, res) {
        res.forEach(function(data){
          timezone = moment().tz("Asia/Shanghai").format('Z');
          data["timestamp"]  = moment(data["result"]["timestamp"]).format('YYYY-MM-DDTHH:mm') + timezone;
          delete data["result"]["timestamp"];
        });
        parseAndCalculateRes([res], err, callback);
      }, req);
 }else if(dataSource.mysql_unconsumed){
      EXPORTS.requestMysqlData(EXPORTS.constructUnconsumedarticlenum(q),function(err, res) {
        res.forEach(function(data){
          timezone = moment().tz("Asia/Shanghai").format('Z');
          data["timestamp"]  = moment(data["result"]["timestamp"]).format('YYYY-MM-DDTHH:mm') + timezone;
          delete data["result"]["timestamp"];
        });
        parseAndCalculateRes([res], err, callback);
      }, req);
 }else if(dataSource.mysql_appsflyer){
   EXPORTS.requestMysqlData(EXPORTS.constructAppsflyerSql(q),function(err, res) {
     res.forEach(function(data){
       timezone = moment().tz("Asia/Shanghai").format('Z');
       data["timestamp"]  = moment(data["result"]["timestamp"]).format('YYYY-MM-DDTHH:mm') + timezone;
       delete data["result"]["timestamp"];
     });
     parseAndCalculateRes([res], err, callback);
   }, req);
 }else if(dataSource.mysql_ballot_user){
   EXPORTS.requestMysqlData(EXPORTS.constructBallotUserSql(q),function(err, res) {
     res.forEach(function(data){
       timezone = moment().tz("Asia/Shanghai").format('Z');
       data["timestamp"]  = moment(data["result"]["timestamp"]).format('YYYY-MM-DDTHH:mm') + timezone;
       delete data["result"]["timestamp"];
     });
     parseAndCalculateRes([res], err, callback);

   }, req);
 }else if(dataSource.mysql_nultiservice){
   EXPORTS.requestMysqlData(EXPORTS.constructMultiServiceUserSql(q), function(err, res) {
     res.forEach(function(data){
       timezone = moment().tz("Asia/Shanghai").format('Z');
       data["timestamp"]  = moment(data["result"]["timestamp"]).format('YYYY-MM-DDTHH:mm') + timezone;
       delete data["result"]["timestamp"];
     });
     parseAndCalculateRes([res], err, callback);

   }, req);
 }else if(dataSource.mysql5){
            EXPORTS.requestMysqlData(EXPORTS.constructFeederSql(q), function(err, res) {
                res.forEach(function(data){
                    timezone = moment().tz("Asia/Shanghai").format('Z');
                    if(q.granularity == 'timestamp_min'){
                        data["timestamp"]  = moment(data["result"]["timestamp_min"]).format('YYYY-MM-DDTHH:mm') + timezone;
                        delete data["result"]["timestamp_min"];
                    }
                    if(q.granularity == 'timestamp_hour'){
                        data["timestamp"]  = moment(data["result"]["timestamp_hour"]).format('YYYY-MM-DDTHH:mm') + timezone;
                        delete data["result"]["timestamp_hour"];
                    }
                    if(q.granularity == 'timestamp_day'){
                        data["timestamp"]  = moment(data["result"]["timestamp_day"]).format('YYYY-MM-DDTHH:mm') + timezone;
                        delete data["result"]["timestamp_day"];
                    }
                });
                parseAndCalculateRes([res], err, callback);
            }, req);
 }else if(dataSource.mysql13){
            EXPORTS.requestMysqlData(EXPORTS.constructThroughput(q), function(err, res) {
                res.forEach(function(data){
                    timezone = moment().tz("Asia/Shanghai").format('Z');
                    if(q.granularity == 'timestamp_min'){
                       data["timestamp"]  = moment(data["result"]["timestamp_min"]).format('YYYY-MM-DDTHH:mm') + timezone;
                       delete data["result"]["timestamp_min"];
                    }
                    if(q.granularity == 'timestamp_hour'){
                       data["timestamp"]  = moment(data["result"]["timestamp_hour"]).format('YYYY-MM-DDTHH:mm') + timezone;
                       delete data["result"]["timestamp_hour"];
                    }
                    if(q.granularity == 'timestamp_day'){
                       data["timestamp"]  = moment(data["result"]["timestamp_day"]).format('YYYY-MM-DDTHH:mm') + timezone;
                       delete data["result"]["timestamp_day"];
                    }
                });
                parseAndCalculateRes([res], err, callback);
            }, req);

    }else if(dataSource.mysql14){
            EXPORTS.requestMysqlData(EXPORTS.constructPublish(q),function(err, res) {
                res.forEach(function(data){
                    timezone = moment().tz("Asia/Shanghai").format('Z');
                    if(q.granularity == 'timestamp_min'){
                       data["timestamp"]  = moment(data["result"]["timestamp_min"]).format('YYYY-MM-DDTHH:mm') + timezone;
                       delete data["result"]["timestamp_min"];
                    }
                    if(q.granularity == 'timestamp_hour'){
                       data["timestamp"]  = moment(data["result"]["timestamp_hour"]).format('YYYY-MM-DDTHH:mm') + timezone;
                       delete data["result"]["timestamp_hour"];
                    }
                    if(q.granularity == 'timestamp_day'){
                       data["timestamp"]  = moment(data["result"]["timestamp_day"]).format('YYYY-MM-DDTHH:mm') + timezone;
                       delete data["result"]["timestamp_day"];
                    }
                });
                parseAndCalculateRes([res], err, callback);
            }, req);

    }else if(dataSource.mysql7){
        EXPORTS.requestMysqlData(EXPORTS.constructNewUserMonitor(q), function(err, res) {
            res.forEach(function(data){
                timezone = moment().tz("Asia/Shanghai").format('Z');
                data["timestamp"]  = moment(data["result"]["timestamp"]).format('YYYY-MM-DDTHH:mm') + timezone;
                delete data["result"]["timestamp"];
            });
             parseAndCalculateRes([res], err, callback);
       }, req);
   }else if(dataSource.mysql11){
        EXPORTS.requestMysqlData(EXPORTS.constructPush(q), function(err, res) {
            res.forEach(function(data){
                timezone = moment().tz("Asia/Shanghai").format('Z');
                data["timestamp"]  = moment(data["result"]["timestamp"]).format('YYYY-MM-DDTHH:mm') + timezone;
                delete data["result"]["timestamp"];
            });
             parseAndCalculateRes([res], err, callback);
       }, req);
 }else if(dataSource.mysql9){
   var table = "t_daily_user",
       timeNum =  Math.floor(((new Date()).getTime() - (new Date(q.startTime)).getTime())/(24*60*60*1000)),
       metrics = ["total_request","total_request_user","total_impression","total_impression_user","total_click","total_click_user","total_detail_dwell_time","total_detail_dwell_user","total_listpage_dwelltime","total_listpage_dwelltime_user"];
   if(timeNum > 15){
     if(!q["session_id"]){
       q["session_id"] = state.env['ipaddress'] +"_"+ new Date(moment().format('YYYY-MM-DD HH:mm:00')).getTime();
     }
     isGetCompared = false;
     EXPORTS.requestHttpData(EXPORTS.constructhttpRequestBody(q, metrics,table), function(err, data) {
       if (err) {
         console.error(err);
         res.json({
           status: 500,
           content: {},
           msg:  'To get httpRequest failed!'
         });
         return;
       }
       res.json({
         status: 200,
         msg:"task id:" + data["task_id"]
       });
     },req);
   }else{
     EXPORTS.requestMysqlData(EXPORTS.constructIndex(q),function(err, res) {
       res.forEach(function(data){
         timezone = moment().tz("Asia/Shanghai").format('Z');
         data["timestamp"]  = moment(data["result"]["timestamp"]).format('YYYY-MM-DDTHH:mm') + timezone;
         delete data["result"]["timestamp"];
       });
       parseAndCalculateRes([res], err, callback);
     }, req);
   }
 }else if(dataSource.mysql6){
       EXPORTS.requestMysqlData(EXPORTS.constructMonitorSql(q), function(err, res) {
           res.forEach(function(data){
               timezone = moment().tz("Asia/Shanghai").format('Z');
               data["timestamp"]  = moment(data["result"]["timestamp"]).format('YYYY-MM-DDTHH:mm') + timezone;
               delete data["result"]["timestamp"];
           });
           parseAndCalculateRes([res], err, callback);
       }, req);

   }else  if (dataSource.druid && dataSource.mysql) {
      EXPORTS.requestDruidData(EXPORTS.constructDruidPostBody(q), function(err, res1) {
        EXPORTS.requestMysqlData(EXPORTS.constructSql(q), function(err2, res2) {
          parseAndCalculateRes([res1, res2], err || err2, callback);
        }, req);
      });
  }else if (dataSource.druid) {
        EXPORTS.requestDruidData(EXPORTS.constructDruidPostBody(q), function(err, res) {
            if(q.dtaSource = "index_user_retain"){
          res.forEach(function(data){
            if(data["result"]["retention_rate_1d"]){
              data["result"]["retention_rate_1d"]  = data["result"]["retention_rate_1d"]*100;
            }
            if(data["result"]["retention_rate_2d"]){
              data["result"]["retention_rate_2d"]  = data["result"]["retention_rate_2d"]*100;
            }
            if(data["result"]["retention_rate_3d"]){
              data["result"]["retention_rate_3d"]  = data["result"]["retention_rate_3d"]*100;
            }
            if(data["result"]["retention_rate_4d"]){
              data["result"]["retention_rate_4d"]  = data["result"]["retention_rate_4d"]*100;
            }
            if(data["result"]["retention_rate_5d"]){
              data["result"]["retention_rate_5d"]  = data["result"]["retention_rate_5d"]*100;
            }
            if(data["result"]["retention_rate_6d"]){
              data["result"]["retention_rate_6d"]  = data["result"]["retention_rate_6d"]*100;
            }
            if(data["result"]["retention_rate_7d"]){
              data["result"]["retention_rate_7d"]  = data["result"]["retention_rate_7d"]*100;
            }
            if(data["result"]["retention_rate_15d"]){
              data["result"]["retention_rate_15d"]  = data["result"]["retention_rate_15d"]*100;
            }
            if(data["result"]["retention_rate_30d"]){
              data["result"]["retention_rate_30d"]  = data["result"]["retention_rate_30d"]*100;
            }
            if(data["result"]["tracking_total_user"]<0){
              data["result"]["tracking_total_user"] = 0;
            }
          });
        }
        if(q.dtaSource = "gmp_monitor"){
          res.forEach(function(data){
            if(data["result"]["max_gmp"] == "-Infinity") {
              data["result"]["max_gmp"] = 0;
            }
          });
        }
        parseAndCalculateRes([res], err, callback);
      });
    } else {
      EXPORTS.requestMysqlData(EXPORTS.constructSql(q), function(err, res) {
        parseAndCalculateRes([res], err, callback);
      }, req);
    }
  }
  query.dataType = chart;
  combinationFilterList = EXPORTS.getFilterCombinationList(filter);

  if (!isGetCompared) {
    combinationFilterList.forEach(function(e) {

      var newQuery = EXPORTS.objectCopyDeep(query),
        queryKey = '',
        i;
      if (_.isEmpty(e)) {

        queryKey = '*';
      } else {
        for (i in e) {
          queryKey += (i + ':' + e[i].values[0] + '\t');
        }
      }
      newQuery.filter = e;
      asyncFunc[queryKey] = function(callback) {
        requestData(newQuery, callback);
      };
    });
  } else {
    asyncFunc['yesterday'] = function(callback) {
      var newQuery = EXPORTS.objectCopyDeep(query);
      newQuery.startTime = EXPORTS.adjustTimeStrByDay(query.startTime, -1);
      newQuery.endTime = EXPORTS.adjustTimeStrByDay(query.endTime, -1);
      requestData(newQuery, callback);
    };
    asyncFunc['lastweek'] = function(callback) {
      var newQuery = EXPORTS.objectCopyDeep(query);
      newQuery.startTime = EXPORTS.adjustTimeStrByDay(query.startTime, -7);
      newQuery.endTime = EXPORTS.adjustTimeStrByDay(query.endTime, -7);
      requestData(newQuery, callback);
    };
  }

  async.parallel(asyncFunc, function(err, result) {
    if (err) {
      console.error(err);
      res.json({
        status: 500,
        content: {},
        msg: 'Failed to get the data!'
      });
      return;
    }


    res.json({
      status: 200,
      content: result,
      msg: 'success!'
    });
  });
});

router.post('/data/table_only/:chart', function(req, res) {

  var query = req.body || {},
    chart = req.params.chart,
    config = EXPORTS.getChartConf(chart),
    dataSource = EXPORTS.getChartDataSourceConf(chart);

  function calcDerivedData(data, chartName) {

    // id to name map
    var idNameMap = EXPORTS.getChartMetricsIdNameMap(chartName),
      calculateMetrics = EXPORTS.getChartMetricsNeedCalculate(chartName);
    // 处理每个时间点httpRequest
    data.forEach(function(item) {
      var time = new Date(item.timestamp).getTime(),
        result = item && (item.result || item.event) || [],
        records = _.isArray(result) && result || [result];

      // 处理table里面的每一条记录
      records.forEach(function(record) {
        calculateMetrics.forEach(function(cm) {
          var evalState = '',
            dependency = cm.dependency || [],
            mid,
            mname,
            j;
            for (j = 0; j < dependency.length; j++) {
              mid = dependency[j]; // metrics id
              mname = idNameMap[mid].name; // metrics name (field names in druid or MySQL)
              evalState += ('var ' + mid + ' = ' + record[mname] + ';');
            }
            evalState += 'expressR = ' + cm.expression + ';';

            try {
              eval(evalState);
              if (!isNaN(expressR)) {
                record[cm.name] = expressR;
              }else {
                record[cm.name] = 0;
              }
            }
            catch(exception) {
              record[cm.name] = 0;
            }
        });// calculatedMetrics foreach
         if(record['article_id']){
              record['id'] = record['article_id'];
          }
        // 处理href
        _.keys(record).forEach(function(rkey) {
          if (_.has(idNameMap[rkey],'href')) {
            var rvalue = record[rkey], hvalue = record[idNameMap[rkey].href];
            record[rkey] = '<a href="' + hvalue + '" target="_blank"'+ '  title ="'+rvalue+'" >' + rvalue + '</a>';
          }
         });
    });// records foreach

    });// data foreach

    return data;
  }

  if (!config) {
    res.json({
      status: 400,
      content: {},
      msg: 'Metric不存在!'
    });
    return;
  }

  if (query.groupBy && query.groupBy.length >= 4) {
    res.json({
      status: 400,
      content: {},
      msg: '为保护后台数据系统，Groupby维度请不要大于4!'
    });
    return;
  }

  function parseAndCalculateRes(resList, err, callback) {
    if (err) {
      console.error(err)
      callback(err);
      return;
    }

    var derivedRes = calcDerivedMetrics(resList, chart),
      pointCount = derivedRes.length,
      sumRes = {},
      idNameMap = EXPORTS.getChartMetricsIdNameMap(chart),
      metricIDs = _.keys(idNameMap);

    metricIDs.forEach(function(mid) { 
      mname = idNameMap[mid].name;
      sumRes[mname] = 0; 
    });

    derivedRes.forEach(function(point){
      var values = point.result;
      metricIDs.forEach(function(mid){
        mname = idNameMap[mid].name;
        values[mname] = values[mname] || 0;
        sumRes[mname] += (values[mname] / pointCount);
      });
    });

    metricIDs.forEach(function(mid) {
      mname = idNameMap[mid].name;
      sumRes[mname] = sumRes[mname]>1000 && Math.round(sumRes[mname]) || sumRes[mname];
    });

    callback(null, {
      data: derivedRes,
      sum: sumRes
    });
  }


  function mergeDruidMysql(druidData,mysqlData){ 
        var titles = [];
	druidData.forEach(function(mDruidData){
                var result =mDruidData['result'];
                result.forEach(function (data1) {
                    var article_id = data1["content_id"];
                    mysqlData.forEach(function(oneSqlData){
                          if(article_id == oneSqlData["result"]["content_id"]){
                                data1['title'] =  '<a href="' + oneSqlData["result"]["url"] + '" target="_blank"'+ '  title ="'+oneSqlData["result"]["title"]+'" >' + oneSqlData["result"]["title"] + '</a>';
                                //data1["title"] = oneSqlData["result"]["title"];
                           }
                       });
                 if(typeof(data1['title']) == "undefined"){
                        data1['title'] = "-";
                 }
                 data1['content_id'] = '<a href="http://cms.hotoday.in/newsContent/index?contentId='+article_id +'&lock=1" target="_blank" title="'+article_id+'">'+article_id +'</a>';
               });
	});
	return druidData;
   }

  query.dataType = chart;
  

if (dataSource.druid && dataSource.mysql) {
    if((new Date(query.endTime)-new Date(query.startTime)) > 600000 && query.granularity == '10minute' && query.groupBy == 'content_id' && !query.filter["content_id"]){
      res.json({
        status: 500,
        content: [],
        msg: '时间区间不正确!'
      });
    }else{
      EXPORTS.requestDruidData(EXPORTS.constructDruidPostBody(query), function(err, res1) {

            var queryName =query['groupBy'][0];
            var expids = [];
            var  queryInfo = {};
            queryInfo['name'] = "id";
            res1 = calcDerivedData(res1, chart);

            res1.forEach(function(data){
                var result =data['result'];

                    result.forEach(function (data1) {
                        //计算ctr
                        if(data1["click"]&&data1["impression"]){
                             data1["ctr"] = (data1["click"]*100/data1["impression"]).toFixed(2)+"%";
                        }else{
                             data1["ctr"] = 0+"%";
                        }
                        if(data1["click"]&&data1["dwelltime"]){
                             data1["avg_dwelltime"] = (data1["dwelltime"]/data1["click"]).toFixed(2);
                        }else {
                             data1["avg_dwelltime"] = 0;
                        }
                        var expid = data1[queryName];
                       // expids.push(expid);
                        expids.push("'"+expid+"'");
                    });
            });
       
            queryInfo['values'] = expids;
            query['id']=queryInfo;
        
            var mysql = require('mysql');
            var conn = mysql.createConnection({
              host: 'db-mta-us-east-1.cxleyzgw272j.us-east-1.rds.amazonaws.com',
              user: 'dashboard_r',
              password: '4d4416cb2d44e38073110b194613c020',
              database:'db_mta',
              port: 3306
            });
            EXPORTS.requestOverallData(EXPORTS.constructSqlTableOnly(expids),conn, function(err2, res2) {
               result = mergeDruidMysql(res1, res2);
               res.json({
                  status: 200,
                  content: result,
                  msg: 'success!'
               });          
           }, req);
       });
   }
}//else if (dataSource.druid) {
    // Table format only request Druid Data
   // EXPORTS.requestDruidData(EXPORTS.constructDruidPostBody(query), function(err, data) {
   //   if (err) {
   //     console.error(err);
   //     res.json({
   //       status: 500,
   //       content: {},
   //       msg: '后台获取Druid数据失败!'
    //    });
    //    return;
  //    }
 //     derivedData = calcDerivedData(data, chart);
 //     res.json({
 //       status: 200,
 //       content: derivedData,
 //       msg: '获取数据成功!'
 //     });
 //   });
//}
else if (dataSource.druid) {
    EXPORTS.requestDruidData(EXPORTS.constructDruidPostBody(query), function(err, data) {
      if (err) {
        console.error(err);
        res.json({
          status: 500,
          content: {},
          msg: 'To get druid data failed!'
        });
        return;
      }
      derivedData = calcDerivedData(data, chart);
      if(query.dataSource = "index_table"){
      derivedData.forEach(function(d){
       if(d["result"][0]){
        if(d["result"][0]["retention_rate_1d"]){
          d["result"][0]["retention_rate_1d"] = (d["result"][0]["retention_rate_1d"]*100).toFixed(2)+"%";
        }
        if(d["result"][0]["retention_rate_2d"]){
          d["result"][0]["retention_rate_2d"]  = (d["result"][0]["retention_rate_2d"]*100).toFixed(2)+"%";
        }
        if(d["result"][0]["retention_rate_3d"]){
          d["result"][0]["retention_rate_3d"]  = (d["result"][0]["retention_rate_3d"]*100).toFixed(2)+"%";
        }
        if(d["result"][0]["retention_rate_4d"]){
          d["result"][0]["retention_rate_4d"]  = (d["result"][0]["retention_rate_4d"]*100).toFixed(2)+"%";
        }
        if(d["result"][0]["retention_rate_5d"]){
          d["result"][0]["retention_rate_5d"]  = (d["result"][0]["retention_rate_5d"]*100).toFixed(2)+"%";
        }
        if(d["result"][0]["retention_rate_6d"]){
          d["result"][0]["retention_rate_6d"]  = (d["result"][0]["retention_rate_6d"]*100).toFixed(2)+"%";
        }
        if(d["result"][0]["retention_rate_7d"]){
          d["result"][0]["retention_rate_7d"]  = (d["result"][0]["retention_rate_7d"]*100).toFixed(2)+"%";
        }
        if(d["result"][0]["retention_rate_15d"]){
          d["result"][0]["retention_rate_15d"]  = (d["result"][0]["retention_rate_15d"]*100).toFixed(2)+"%";
        }
        if(d["result"][0]["retention_rate_30d"]){
          d["result"][0]["retention_rate_30d"]  = (d["result"][0]["retention_rate_30d"]*100).toFixed(2)+"%";
        }
       }
      });}
      res.json({
        status: 200,
        content: derivedData,
        msg: 'success!'
      });
    }, req);
}else if(dataSource.mysql_country){
    EXPORTS.requestMysqlData(EXPORTS.constructUserCountry(query), function(err2, data) {
      data.forEach(function(d){
        timezone = moment().tz("Asia/Shanghai").format('Z');
        d["timestamp"]  = moment(d["result"]["timestamp"]).format('YYYY-MM-DDTHH:mm') + timezone;
      });
      derivedData = calcDerivedData(data,chart);
      res.json({
        status: 200,
        content: derivedData,
        msg: 'success!'
      });
    }, req);
}else if(dataSource.mysql_pushserver_new){
    var mysql = require('mysql');
    var conn = mysql.createConnection({
      host: 'db-mta-us-east-1.cxleyzgw272j.us-east-1.rds.amazonaws.com',
      user: 'dashboard_r',
      password: '4d4416cb2d44e38073110b194613c020',
      database:'db_mta',
      port: 3306
    });
    EXPORTS.requestOverallData(EXPORTS.constructPushServerNew(query), conn,function(err2, data) {
      data.forEach(function(d){
        timezone = moment().tz("Asia/Shanghai").format('Z');
        d["timestamp"]  = moment(d["result"]["timestamp"]).format('YYYY-MM-DDTHH:mm') + timezone;
      });
      var time = new Date("2017-08-04 15:40:00");
      derivedData = calcDerivedData(data,chart);
      derivedData.forEach(function(d1){
        d1["result"]["timestamp"] = d1["result"]["timestamp"].getTime();
        if(d1["result"]["promotion_appver_tag"] == 1){
          d1["result"]["promotion_appver_tag"] = "gp";
        }else if(d1["result"]["promotion_appver_tag"] == 2){
          if(d1["result"]["timestamp"]<time){
            d1["result"]["promotion_appver_tag"] = "appiaint_new";
          }else{
            d1["result"]["promotion_appver_tag"] = "appiaint_2.5.4.1.0.6";
          }
        }else if(d1["result"]["promotion_appver_tag"] == 3){
          if(d1["result"]["timestamp"]<time){
            d1["result"]["promotion_appver_tag"] = "appiaint_old";
          }else{
            d1["result"]["promotion_appver_tag"] = "appiaint_below2.5.4.1.0.6";
          }
        }else{
          d1["result"]["promotion_appver_tag"] = "";
        }
        if(d1["result"]["push_impression_30mins"]&&d1["result"]["push_click_30mins"]){
          d1["result"]["ctr_push_30mins"] = (d1["result"]["push_click_30mins"]*100/d1["result"]["push_impression_30mins"]).toFixed(2)+"%";
        }else{
          d1["result"]["ctr_push_30mins"] = 0;
        }
        if(d1["result"]["push_impression_2hours"]&&d1["result"]["push_click_2hours"]){
          d1["result"]["ctr_push_2hours"] = (d1["result"]["push_click_2hours"]*100/d1["result"]["push_impression_2hours"]).toFixed(2)+"%";
        }else{
          d1["result"]["ctr_push_2hours"] = 0;
        }
        if(d1["result"]["push_impression_24hours"]&&d1["result"]["push_click_24hours"]){
          d1["result"]["ctr_push_24hours"] = (d1["result"]["push_click_24hours"]*100/d1["result"]["push_impression_24hours"]).toFixed(2)+"%";
        }else{
          d1["result"]["ctr_push_24hours"] = 0;
        }
        if(d1["result"]["content_id"]&&d1["result"]["id"]){
          if(d1["result"]["strategy"] == "push_explore"){
            d1["result"]["content_id"] = '<a href="http://cms.hotoday.in/newsContent/index?lock=1&refer=dashboard&contentId='+d1["result"]["content_id"] +'" target="_blank" title="'+d1["result"]["content_id"]+'">'+d1["result"]["content_id"]+'</a>&nbsp;&nbsp;<a href="http://cms.hotoday.in/PushExploreNews/pushNewsDetail?timeZoneName=Asia/Shanghai&info=true&refer=dashboard&id='+d1["result"]["autoinc_push_news_id"] +'" target="_blank" title="'+d1["result"]["autoinc_push_news_id"]+'">pushDetail</a>';
          }else{
            d1["result"]["content_id"] = '<a href="http://cms.hotoday.in/newsContent/index?lock=1&refer=dashboard&contentId='+d1["result"]["content_id"] +'" target="_blank" title="'+d1["result"]["content_id"]+'">'+d1["result"]["content_id"]+'</a>&nbsp;&nbsp;<a href="http://cms.hotoday.in/NewsPush/pushNewsDetail?timeZoneName=Asia/Shanghai&info=true&refer=dashboard&id='+d1["result"]["autoinc_push_news_id"] +'" target="_blank" title="'+d1["result"]["autoinc_push_news_id"]+'">pushDetail</a>';
          }
        }
      });
      res.json({
        status: 200,
        content: derivedData,
        msg: 'success!'
      });
    }, req);
  }else if(dataSource.mysql_ad_report){
    EXPORTS.requestMysqlData(EXPORTS.construcADReportServer(query), function(err2, data) {
      data.forEach(function(d){
        timezone = moment().tz("Asia/Shanghai").format('Z');
        d["timestamp"]  = moment(d["result"]["timestamp"]).format('YYYY-MM-DDTHH:mm') + timezone;
      });
      derivedData = calcDerivedData(data,chart);
      res.json({
        status: 200,
        content: derivedData,
        msg: 'success!'
      });
    }, req);
 }else if(dataSource.mysql_articesharenum){
    EXPORTS.requestMysqlData(EXPORTS.constructArticleSharenumSql(query), function(err2, data) {
      data.forEach(function(d){
        timezone = moment().tz("Asia/Shanghai").format('Z');
        d["timestamp"]  = moment(d["result"]["timestamp"]).format('YYYY-MM-DDTHH:mm') + timezone;
        delete d["result"]["timestamp"];
      });
      derivedData = calcDerivedData(data,chart);
      res.json({
        status: 200,
        content: derivedData,
        msg: '获取数据成功!'
      });
    },req);
}else if(dataSource.mysql_groupsharenum){
    EXPORTS.requestMysqlData(EXPORTS.constructSharetogroupSql(query), function(err2, data) {
      data.forEach(function(d){
        timezone = moment().tz("Asia/Shanghai").format('Z');
        d["timestamp"]  = moment(d["result"]["timestamp"]).format('YYYY-MM-DDTHH:mm') + timezone;
        delete d["result"]["timestamp"];
      });
      derivedData = calcDerivedData(data,chart);
      res.json({
        status: 200,
        content: derivedData,
        msg: 'success!'
      });
    },req);
}else if(dataSource.mysql_h5pv){
    var mysql = require('mysql');
    var conn = mysql.createConnection({
      host: 'dashboard.cxleyzgw272j.us-east-1.rds.amazonaws.com',
      user: 'dashboard_w',
      password: '745e0390deb0abe4c46bbe3f13efa490',
      database:'dashboard',
      port: 3306
    });
    EXPORTS.requestOverallData(EXPORTS.constructH5SharePVSql(query),conn, function(err2, data) {
      var expids = [];
      data.forEach(function(d){
        timezone = moment().tz("Asia/Shanghai").format('Z');
        d["timestamp"]  = moment(d["result"]["timestamp"]).format('YYYY-MM-DDTHH:mm') + timezone;
        delete d["result"]["timestamp"];
        if(d["result"]["content_id"]){
          expids.push("'"+d["result"]["content_id"]+"'");
        }
      });
      derivedData = data;//calcDerivedData(data,chart);

      var conn2 = mysql.createConnection({
        host: 'db-mta-us-east-1.cxleyzgw272j.us-east-1.rds.amazonaws.com',
        user: 'dashboard_r',
        password: '4d4416cb2d44e38073110b194613c020',
        database:'db_mta',
        port: 3306
      });
      EXPORTS.requestOverallData(EXPORTS.constructSqlTableOnly(expids),conn2,function(err2, res2) {
        derivedData.forEach(function(mDruidData){
          var article_id = mDruidData["result"]["content_id"];
          res2.forEach(function(mysql){
            if(mysql["result"]["content_id"] == article_id){
              mDruidData["result"]['title'] =  '<a href="' + mysql["result"]["url"] + '" target="_blank"'+ '  title ="'+mysql["result"]["title"]+'" >' + mysql["result"]["title"] + '</a>';
            }
          });
          if(typeof(mDruidData["result"]['title']) == "undefined"){
            mDruidData["result"]['title'] = "-";
          }
          if(mDruidData["result"]['page_view']&&mDruidData["result"]['h5_url_click']){
            mDruidData["result"]['pageview_under_urlclick'] = (mDruidData["result"]['page_view']/mDruidData["result"]['h5_url_click']).toFixed(2);
          }else{
            mDruidData["result"]['pageview_under_urlclick'] = 0;
          }
          if(mDruidData["result"]['click_download_app']&&mDruidData["result"]['page_view']){
            mDruidData["result"]['clickdownload_under_pageview'] =( mDruidData["result"]['click_download_app']/mDruidData["result"]['page_view']).toFixed(2);
          }else{
            mDruidData["result"]['clickdownload_under_pageview'] = 0;
          }
          if(mDruidData["result"]['install_num']&&mDruidData["result"]['click_download_app']){
            mDruidData["result"]['install_under_download'] = (mDruidData["result"]['install_num']/mDruidData["result"]['click_download_app']).toFixed(2);
          }else{
            mDruidData["result"]['install_under_download'] = 0;
          }
          mDruidData["result"]["content_id"] = '<a href="http://cms.hotoday.in/newsContent/index?contentId='+article_id +'" target="_blank" title="'+mDruidData["result"]["content_id"]+'">'+article_id +'</a>';
        });
        res.json({
          status: 200,
          content: derivedData,
          msg: 'success!'
        });
      }, req);
    });
}else if(dataSource.mysql_h5uv){
    EXPORTS.requestMysqlData(EXPORTS.constructH5ShareUVSql(query), function(err2, data) {
      var expids = [];
      data.forEach(function(d){
        timezone = moment().tz("Asia/Shanghai").format('Z');
        d["timestamp"]  = moment(d["result"]["timestamp"]).format('YYYY-MM-DDTHH:mm') + timezone;
        delete d["result"]["timestamp"];
        if(d["result"]['download_app_user']&&d["result"]['page_view_user']){
            d["result"]['downloaduser_under_pagevireuser'] = (d["result"]['download_app_user']/d["result"]['page_view_user']).toFixed(2);
          }else{
            d["result"]['downloaduser_under_pagevireuser'] = 0;
          }
          if(d["result"]['install_app_user']&&d["result"]['download_app_user']){
            d["result"]['installuser_under_doenloaduser'] =(d["result"]['install_app_user']/d["result"]['download_app_user']).toFixed(2);
          }else{
            d["result"]['installuser_under_doenloaduser'] = 0;
          }
      });
      derivedData = calcDerivedData(data,chart);
      res.json({
        status: 200,
        content: derivedData,
        msg: 'success!'
      });
    },req);
}else if(dataSource.mysql_h5share){
  var mysql = require('mysql');
  var conn = mysql.createConnection({
    host: 'dashboard.cxleyzgw272j.us-east-1.rds.amazonaws.com',
    user: 'dashboard_w',
    password: '745e0390deb0abe4c46bbe3f13efa490',
    database:'dashboard',
    port: 3306
  });
  EXPORTS.requestOverallData(EXPORTS.constructH5ShareSql(query),conn, function(err2, data) {
    var expids = [];
    data.forEach(function(d){
      timezone = moment().tz("Asia/Shanghai").format('Z');
      d["timestamp"]  = moment(d["result"]["timestamp"]).format('YYYY-MM-DDTHH:mm') + timezone;
      delete d["result"]["timestamp"];
      if(d["result"]["content_id"]){
        expids.push("'"+d["result"]["content_id"]+"'");
      }
    });
    derivedData = calcDerivedData(data,chart);

    var conn2 = mysql.createConnection({
      host: 'db-mta-us-east-1.cxleyzgw272j.us-east-1.rds.amazonaws.com',
      user: 'dashboard_r',
      password: '4d4416cb2d44e38073110b194613c020',
      database:'db_mta',
      port: 3306
    });
    EXPORTS.requestOverallData(EXPORTS.constructSqlTableOnly(expids),conn2,function(err2, res2) {
      derivedData.forEach(function(mDruidData){
        var article_id = mDruidData["result"]["content_id"];
        res2.forEach(function(mysql){
          if(mysql["result"]["content_id"] == article_id){
            mDruidData["result"]['title'] =  '<a href="' + mysql["result"]["url"] + '" target="_blank"'+ '  title ="'+mysql["result"]["title"]+'" >' + mysql["result"]["title"] + '</a>';
          }
        });
        if(typeof(mDruidData["result"]['title']) == "undefined"){
          mDruidData["result"]['title'] = "-";
        }
        mDruidData["result"]["content_id"] = '<a href="http://cms.hotoday.in/newsContent/index?contentId='+article_id +'" target="_blank" title="'+mDruidData["result"]["content_id"]+'">'+article_id +'</a>';
      });
      res.json({
        status: 200,
        content: derivedData,
        msg: 'success!'
      });
    }, req);
  });
}else if(dataSource.mysql_histority){
  EXPORTS.requestMysqlData(EXPORTS.constructHistoricalSql(query), function(err2, data) {
    data.forEach(function(d){
      timezone = moment().tz("Asia/Shanghai").format('Z');
      d["timestamp"]  = moment(d["result"]["submit_time"]).format('YYYY-MM-DDTHH:mm') + timezone;
    });
    derivedData = calcDerivedData(data,chart);
    derivedData.forEach(function(d){
      if(d["result"]["output_path"]!="NULL"){
        d["result"]["output_path"] = "<a target='_blank' href='"+d["result"]["output_path"]+"'>"+d['result']['output_path']+"</a>";
      }
    });
    res.json({
      status: 200,
      content: derivedData,
      msg: 'success!'
    });
  }, req);
}else if(dataSource.mysql_ad_daily){
        EXPORTS.requestMysqlData(EXPORTS.constructAdDaily(query),function(err, data) {
            if(err){
                console.error(err);
                res.json({
                    status:500,
                    content:{},
                    msg:'To get mysql data failed!'
                });
                return;
            }
            data.forEach(function(d){
                timezone = moment().tz("Asia/Shanghai").format('Z');
                d["timestamp"]  = moment(d["result"]["timestamp"]).format('YYYY-MM-DDTHH:mm') + timezone;
                delete d["result"]["timestamp"];
            });
            var  derivedData = calcDerivedData(data,chart);
            derivedData.forEach(function(d){
                if(d["result"]["ad_demand_fill_under_ad_demand"] && d["result"]["ad_demand_fill_under_ad_demand"] !="Infinity"){
                    d["result"]["ad_demand_fill_under_ad_demand"] = (d["result"]["ad_demand_fill_under_ad_demand"]*100).toFixed(2)+"%";
                }else{
                    d["result"]["ad_demand_fill_under_ad_demand"] = 0 ;
                }
                if(d["result"]["click_under_impression"] && d["result"]["click_under_impression"] !="Infinity"){
                    d["result"]["click_under_impression"] = (d["result"]["click_under_impression"]*100).toFixed(2)+"%";
                }else{
                    d["result"]["click_under_impression"] = 0;
                }
                if(d["result"]["ad_request_fill_under_ad_request"] && d["result"]["ad_request_fill_under_ad_request"] !="Infinity"){
                    d["result"]["ad_request_fill_under_ad_request"] = (d["result"]["ad_request_fill_under_ad_request"]*100).toFixed(2)+"%";
                }else{
                    d["result"]["ad_request_fill_under_ad_request"] = 0;
                }
                if(d["result"]["impression_under_ad_request_fill"] && d["result"]["impression_under_ad_request_fill"] !="Infinity"){
                    d["result"]["impression_under_ad_request_fill"] = (d["result"]["impression_under_ad_request_fill"]*100).toFixed(2)+"%";
                }else{
                    d["result"]["impression_under_ad_request_fill"] = 0;
                }
                if(d["result"]["impression_under_adspace_impression"] && d["result"]["impression_under_adspace_impression"] !="Infinity"){
                    d["result"]["impression_under_adspace_impression"] = (d["result"]["impression_under_adspace_impression"]*100).toFixed(2)+"%";
                }else{
                    d["result"]["impression_under_adspace_impression"] = 0;
                }
            });
            res.json({
                status:200,
                content:derivedData,
                msg:'success!'
            });
        }, req);
}else if(dataSource.mysql_h5){
  var mysql = require('mysql');
  var conn = mysql.createConnection({
    host: 'dashboard.cxleyzgw272j.us-east-1.rds.amazonaws.com',
    user: 'dashboard_w',
    password: '745e0390deb0abe4c46bbe3f13efa490',
    database:'dashboard',
    port: 3306
  });
  EXPORTS.requestOverallData(EXPORTS.constructDailyH5Sql(query),conn, function(err2, data) {
    var expids = [],
        uids = [];
    data.forEach(function(d){
      timezone = moment().tz("Asia/Shanghai").format('Z');
      d["timestamp"]  = moment(d["result"]["timestamp"]).format('YYYY-MM-DDTHH:mm') + timezone;
      delete d["result"]["timestamp"];
      if(d["result"]["content_id"]){
        expids.push("'"+d["result"]["content_id"]+"'");
      }
      if(d["result"]["uid"]){
        uids.push("'"+d["result"]["uid"]+"'");
      }
    });
    derivedData = calcDerivedData(data,chart);

    var conn2 = mysql.createConnection({
      host: 'db-mta-us-east-1.cxleyzgw272j.us-east-1.rds.amazonaws.com',
      user: 'dashboard_r',
      password: '4d4416cb2d44e38073110b194613c020',
      database:'db_mta',
      port: 3306
    });
    EXPORTS.requestOverallData(EXPORTS.constructSqlTableOnly(expids),conn2,function(err2, res2) {
      derivedData.forEach(function(mDruidData){
        var article_id = mDruidData["result"]["content_id"];
        res2.forEach(function(mysql){
          if(mysql["result"]["content_id"] == article_id){
            mDruidData["result"]['title'] =  '<a href="' + mysql["result"]["url"] + '" target="_blank"'+ '  title ="'+mysql["result"]["title"]+'" >' + mysql["result"]["title"] + '</a>';
          }
        });
        if(typeof(mDruidData["result"]['title']) == "undefined"){
          mDruidData["result"]['title'] = "-";
        }
        mDruidData["result"]["content_id"] = '<a href="http://cms.hotoday.in/newsContent/index?contentId='+article_id +'" title="'+mDruidData["result"]["content_id"]+'" target="_blank">'+article_id +'</a>';
        //mDruidData["timestamp"] = global.moment((new Date(mDruidData["timestamp"]).getTime()+state.env['country2utc'][query.filter["product_id"]["values"]])).format('YYYY-MM-DD HH:mm:ss').toString();
      });
      var conn3 = mysql.createConnection({
          host: 'db-mta-us-east-1.cxleyzgw272j.us-east-1.rds.amazonaws.com',
          user: 'dashboard_r',
          password: '4d4416cb2d44e38073110b194613c020',
          database: 'db_mta',
          port: 3306
        });
        EXPORTS.requestOverallData(EXPORTS.constructmUsername(uids),conn3, function (err3, res3) {
          derivedData.forEach(function (mDruidData3) {
            var uid = mDruidData3["result"]["uid"];
            res3.forEach(function (mysql3) {
              if (mysql3["result"]["uid"] == uid) {
                mDruidData3["result"]['operator'] =  mysql3["result"]["operator"];
                mDruidData3["result"]['platform'] = mysql3["result"]["platform"];
              }
            });
            if (typeof(mDruidData3["result"]['operator']) == "undefined") {
              mDruidData3["result"]['operator'] = "-";
            }
            if (typeof(mDruidData3["result"]['platform']) == "undefined") {
              mDruidData3["result"]['platform'] = "-";
            }
          });
          res.json({
            status: 200,
            content: derivedData,
            msg: 'success!'
          });
        }, req);
      });
    });
}else if(dataSource.mysql_new_ltv){
  EXPORTS.requestMysqlData(EXPORTS.constructmNewuserltvSql(query), function(err2, data) {
    data.forEach(function(d){
      timezone = moment().tz("Asia/Shanghai").format('Z');
      d["timestamp"]  = moment(d["result"]["timestamp"]).format('YYYY-MM-DDTHH:mm') + timezone;
    });
    derivedData = calcDerivedData(data,chart);
    derivedData.forEach(function(d1){
      if(d1["result"]["impression"]&&d1["result"]["users_num"]){
        d1["result"]["avg_impression_ltv"] = (d1["result"]["impression"]/d1["result"]["users_num"]).toFixed(2);
      }else{
        d1["result"]["avg_impression_ltv"] = 0;
      }
      if(d1["result"]["click"]&&d1["result"]["users_num"]){
        d1["result"]["avg_click_ltv"] = (d1["result"]["click"]/d1["result"]["users_num"]).toFixed(2);
      }else{
        d1["result"]["avg_click_ltv"] = 0;
      }
    });
    res.json({
      status: 200,
      content: derivedData,
      msg: 'success!'
    });
  }, req);
}else if(dataSource.mysql_ballot_article){
  EXPORTS.requestMysqlData(EXPORTS.constructBallotArticleSql(query),function(err2, data) {
    data.forEach(function(d){
      timezone = moment().tz("Asia/Shanghai").format('Z');
      d["timestamp"]  = moment(d["result"]["timestamp"]).format('YYYY-MM-DDTHH:mm') + timezone;
    });
    derivedData = calcDerivedData(data,chart);
    derivedData.forEach(function(d1){
      if(d1["result"]["like_num"]&&d1["result"]["click"]){
        d1["result"]["avg_like_click"] = (d1["result"]["like_num"]/d1["result"]["click"]).toFixed(4);
      }else{
        d1["result"]["avg_like_click"] = 0;
      }
      if(d1["result"]["angry_num"]&&d1["result"]["click"]){
        d1["result"]["avg_angry_click"] = (d1["result"]["angry_num"]/d1["result"]["click"]).toFixed(4);
      }else{
        d1["result"]["avg_angry_click"] = 0;
      }
      if(d1["result"]["sad_num"]&&d1["result"]["click"]){
        d1["result"]["avg_sad_click"] = (d1["result"]["sad_num"]/d1["result"]["click"]).toFixed(4);
      }else{
        d1["result"]["avg_sad_click"] = 0;
      }
      if(d1["result"]["boring_num"]&&d1["result"]["click"]){
        d1["result"]["avg_boring_click"] = (d1["result"]["boring_num"]/d1["result"]["click"]).toFixed(4);
      }else{
        d1["result"]["avg_boring_click"] = 0;
      }
      if(d1["result"]["content_id"]){
        d1["result"]["content_id"] = '<a title ="'+d1["result"]["content_id"]+'" >' + d1["result"]["content_id"] + '</a>';
      }
    });
    res.json({
      status: 200,
      content: derivedData,
      msg: 'success!'
    });
  }, req);
}else if(dataSource.mysql_media){
 var table = "t_daily_source",
     timeNum =  Math.floor(((new Date()).getTime() - (new Date(query.startTime)).getTime())/(24*60*60*1000)),
     metrics = ["sum(request) as request","sum(request_user) as request_user","sum(impression) as impression","sum(impression_user) as impression_user","sum(click) as click","sum(click_user) as click_user","sum(dwelltime) as dwelltime","sum(available_article_amount) as available_article_amount"];
   if(timeNum > 15){
     if(!query["session_id"]){
       query["session_id"] = state.env['ipaddress'] +"_"+ new Date(moment().format('YYYY-MM-DD HH:mm:00')).getTime();
     }
     isGetCompared = false;
     EXPORTS.requestHttpData(EXPORTS.constructhttpRequestBody(query, metrics,table), function(err, data) {
       if (err) {
         console.error(err);
         res.json({
           status: 500,
           content: {},
           msg: 'To get httpRequest data failed!'
         });
         return;
       }
       res.json({
         status: 200,
         msg:"task id:" + data["task_id"]
       });
     },req);
   }else{
  EXPORTS.requestMysqlData(EXPORTS.constructmMediapvSql(query), function(err2, data) {
    data.forEach(function(d){
      timezone = moment().tz("Asia/Shanghai").format('Z');
      if(d["result"]["timestamp"]){
        d["timestamp"]  = moment(d["result"]["timestamp"]).format('YYYY-MM-DDTHH:mm') + timezone;
      }
    });
    derivedData = calcDerivedData(data,chart);
    derivedData.forEach(function(d){
      if(d["result"]["request"]&&d["result"]["request_all"]){
        d["result"]["request_rate"] = (d["result"]["request"]/(d["result"]["request_all"])).toFixed(4);
      }
      if(d["result"]["impression"]&&d["result"]["impression_all"]){
        d["result"]["impression_rate"] = (d["result"]["impression"]/(d["result"]["impression_all"])).toFixed(4);
      }
    });
    res.json({
      status: 200,
      content: derivedData,
      msg: 'success!'
    });
  }, req);
  }
}else if(dataSource.mysql_appsflyer){
  EXPORTS.requestMysqlData(EXPORTS.constructAppsflyerSql(query), function(err2, data) {
    data.forEach(function(d){
      timezone = moment().tz("Asia/Shanghai").format('Z');
      if(d["result"]["timestamp"]){
        d["timestamp"]  = moment(d["result"]["timestamp"]).format('YYYY-MM-DDTHH:mm') + timezone;
      }
    });
    derivedData = calcDerivedData(data,chart);
    derivedData.forEach(function(d){
      if(d["result"]["new_user"] && d["result"]["impression_user"]){
        d["result"]["imp_under_apps"] = (d["result"]["impression_user"]/d["result"]["new_user"]).toFixed(2);
      }else{
        d["result"]["imp_under_apps"] = 0;
      }
      if(d["result"]["appsflyer_user"] && d["result"]["impression_user"]){
        d["result"]["imp_under_fly"] = (d["result"]["impression_user"]/d["result"]["appsflyer_user"]).toFixed(2);
      }else{
        d["result"]["imp_under_fly"] = 0;
      }
    });
    res.json({
      status: 200,
      content: derivedData,
      msg: 'success!'
    });
  }, req);
}else if(dataSource.mysql7){
  EXPORTS.requestMysqlData(EXPORTS.constructNewUserMonitor(query), function(err, data) {
    if(err){
      console.error(err);
      res.json({
        status:500,
        content:{},
        msg:'To get mysql data failed!'
      });
      return;
    }
    data.forEach(function(d){
      timezone = moment().tz("Asia/Shanghai").format('Z');
      d["timestamp"]  = moment(d["result"]["timestamp"]).format('YYYY-MM-DDTHH:mm') + timezone;
      delete d["result"]["timestamp"];
    });
    derivedData = calcDerivedData(data,chart);
    derivedData.forEach(function(d2){
      if(d2["result"]["impression_user_under_activity_user"]){
        d2["result"]["impression_user_under_activity_user"]  = d2["result"]["impression_user_under_activity_user"].toFixed(2);
      }
      if(d2["result"]["clickUser_under_impression_user"]){
        d2["result"]["clickUser_under_impression_user"]  = d2["result"]["clickUser_under_impression_user"].toFixed(2);
      }
    });
    res.json({
      status:200,
      content:derivedData,
      msg:'success!'
    });
  }, req);
}else if(dataSource.mysql_clickpush){
  var mysql = require('mysql');
  var conn2 = mysql.createConnection({
    host: 'dashboard.cxleyzgw272j.us-east-1.rds.amazonaws.com',
    user: 'dashboard_w',
    password: '745e0390deb0abe4c46bbe3f13efa490',
    database:'dashboard',
    port: 3306
  });
  EXPORTS.requestOverallData(EXPORTS.constructClickpushsql(query),conn2, function(err, data) {
    if(err){
      console.error(err);
      res.json({
        status:500,
        content:{},
        msg:'To get mysql data failed!'
      });
      return;
    }
    var expids = [];
    data.forEach(function(d){
      timezone = moment().tz("Asia/Shanghai").format('Z');
      d["timestamp"]  = moment(d["result"]["timestamp"]).format('YYYY-MM-DDTHH:mm') + timezone;
      delete d["result"]["timestamp"];
      expids.push("'"+d["result"]["content_id"]+"'");
    });
    derivedData = calcDerivedData(data,chart);

    var mysql = require('mysql');
    var conn = mysql.createConnection({
      host: 'db-mta-us-east-1.cxleyzgw272j.us-east-1.rds.amazonaws.com',
      user: 'dashboard_r',
      password: '4d4416cb2d44e38073110b194613c020',
      database:'db_mta',
      port: 3306
    });
    EXPORTS.requestOverallData(EXPORTS.constructSqlTableOnly(expids),conn,function(err2, res2) {
      derivedData.forEach(function(mDruidData){
        res2.forEach(function(mysql){
          if(mysql["result"]["content_id"] == mDruidData["result"]["content_id"]){
            mDruidData["result"]['title'] =  '<a href="' + mysql["result"]["url"] + '" target="_blank"'+ '  title ="'+mysql["result"]["title"]+'" >' + mysql["result"]["title"] + '</a>';
          }
        });
        if(typeof(mDruidData["result"]['title']) == "undefined"){
          mDruidData["result"]['title'] = "-";
        }
        if(mDruidData["result"]["impression"] && mDruidData["result"]["impression_user"]){
          mDruidData["result"]["per_capita_impression"] = (mDruidData["result"]["impression"]/mDruidData["result"]["impression_user"]).toFixed(2);
        }else{
          mDruidData["result"]["per_capita_impression"] = 0;
        }
        if(mDruidData["result"]["impression_user"] && mDruidData["result"]["click_push_user"]){
          mDruidData["result"]["impre_user_under_push_click_user"] = (mDruidData["result"]["impression_user"]/mDruidData["result"]["click_push_user"]).toFixed(2);
        }else{
          mDruidData["result"]["impre_user_under_push_click_user"] = 0;
        }
        if(mDruidData["result"]["impression_push_user"] && mDruidData["result"]["click_push_user"]){
          mDruidData["result"]["push_utr"] = (mDruidData["result"]["click_push_user"]/mDruidData["result"]["impression_push_user"]).toFixed(2);
        }else{
          mDruidData["result"]["push_utr"] = 0;
        }
        mDruidData["result"]['content_id'] = '<a href="http://cms.hotoday.in/newsContent/index?contentId='+ mDruidData["result"]["content_id"] +'" target="_blank" title ="'+mDruidData["result"]["content_id"]+'">'+ mDruidData["result"]["content_id"] +'</a>';
        mDruidData["result"]["push_time"] = global.moment((new Date(mDruidData["result"]["push_time"]).getTime()+state.env['country2utc'][query.filter["product_id"]["values"]])).format('YYYY-MM-DD HH:mm:ss').toString();
      });
      res.json({
        status: 200,
        content: derivedData,
        msg: 'success!'
      });
    }, req);
  });
}else if(dataSource.mysql_pushclick){
  EXPORTS.requestMysqlData(EXPORTS.constructPushclickSql(query), function(err, data) {
    if(err){
      console.error(err);
      res.json({
        status:500,
        content:{},
        msg:'To get mysql data failed!'
      });
      return;
    }
    data.forEach(function(d){
      timezone = moment().tz("Asia/Shanghai").format('Z');
      d["timestamp"]  = moment(d["result"]["timestamp"]).format('YYYY-MM-DDTHH:mm') + timezone;
      delete d["result"]["timestamp"];
    });
    derivedData = calcDerivedData(data,chart);
    derivedData.forEach(function(d){
      if(d["result"]["click"]&&d["result"]["impression"]){
        d["result"]["ctr"] = (d["result"]["click"]*100/(d["result"]["impression"])).toFixed(2)+"%";
      }else{
        d["result"]["ctr"] = 0+"%";
      }
      if(d["result"]["click"]&&d["result"]["dwelltime"]){
        d["result"]["avg_dwelltime"] = (d["result"]["dwelltime"]/(d["result"]["click"])).toFixed(2);
      }else{
        d["result"]["avg_dwelltime"] = 0;
      }
      if(d["result"]["back_list_user"]&&d["result"]["push_click_user"]){
        d["result"]["B2L"] = (d["result"]["back_list_user"]/(d["result"]["push_click_user"])).toFixed(2);
      }else{
        d["result"]["B2L"] = 0;
      }
      d["result"]["no_back2lisuesr"] = d["result"]["push_click_user"] - d["result"]["back_list_user"];
      if(d["result"]["back_list_user"]&&d["result"]["impression"]){
        d["result"]["avg_impression"] = (d["result"]["impression"]/(d["result"]["back_list_user"])).toFixed(2);
      }else{
        d["result"]["avg_impression"] = 0;
      }
      if(d["result"]["back_list_user"]&&d["result"]["click"]){
        d["result"]["avg_click"] = (d["result"]["click"]/(d["result"]["back_list_user"])).toFixed(2);
      }else{
        d["result"]["avg_click"] = 0;
      }
    });
    res.json({
      status:200,
      content:derivedData,
      msg:'success!'
    });
  }, req);
}else if(dataSource.mysql_push){
      EXPORTS.requestMysqlData(EXPORTS.constructPushUser(query),function(err2, data) {
        data.forEach(function(d){
          timezone = moment().tz("Asia/Shanghai").format('Z');
          d["timestamp"]  = moment(d["result"]["push_time"]).format('YYYY-MM-DDTHH:mm') + timezone;
          delete d["result"]["push_time"];
        });
        derivedData = calcDerivedData(data,chart);
        derivedData.forEach(function(d){
          d["result"]["fialed_rate_overall"] = d["result"]["fialed_rate_overall"].toFixed(2)+"%";
          d["result"]["invalid_rate_new_user"] = d["result"]["invalid_rate_new_user"].toFixed(2)+"%";
        });
        res.json({
          status: 200,
          content: derivedData,
          msg: 'success!'
        });
      }, req);
    }else if(dataSource.mysql_table){
        EXPORTS.requestMysqlData(EXPORTS.constructUserTable(query), function(err, data) {
            if(err){
                console.error(err);
                res.json({
                    status:500,
                    content:{},
                    msg:'To get mysqldata failed!'
                });
                return;
            }
            data.forEach(function(d){
                timezone = moment().tz("Asia/Shanghai").format('Z');
                d["timestamp"]  = moment(d["result"]["timestamp"]).format('YYYY-MM-DDTHH:mm') + timezone;
                delete d["result"]["timestamp"];
            });
            derivedData = calcDerivedData(data,chart);
            derivedData.forEach(function(d){
                d["result"]["avg_dwelltime_under_impression_user"] = d["result"]["avg_dwelltime_under_impression_user"].toFixed(2);
                d["result"]["avg_detail_dwelltime_under_click"] = d["result"]["avg_detail_dwelltime_under_click"].toFixed(2);
                d["result"]["avg_click_under_impression_user"] = d["result"]["avg_click_under_impression_user"].toFixed(2);
                d["result"]["avg_click_under_click_user"] = d["result"]["avg_click_under_click_user"].toFixed(2);
                d["result"]["avg_impression_under_impression_user"] = d["result"]["avg_impression_under_impression_user"].toFixed(2);
                d["result"]["avg_click_under_impression"] = d["result"]["avg_click_under_impression"].toFixed(2);
                d["result"]["avg_list_dwell_time_under_impression_user"] = d["result"]["avg_list_dwell_time_under_impression_user"].toFixed(2);
                d["result"]["avg_request_under_request_user"] = d["result"]["avg_request_under_request_user"].toFixed(2);
                d["result"]["utr"] = d["result"]["utr"].toFixed(2);
            });
            res.json({
                status:200,
                content:derivedData,
                msg:'success!'
            });
        }, req);
 }else if(dataSource.mysql_operate){
  var table = query.dataType,
      timeNum =  Math.floor(((new Date()).getTime() - (new Date(query.startTime)).getTime())/(24*60*60*1000)),
      metrics = ["sum(request) as request","sum(impression) as impression","sum(click) as click","sum(dwelltime) as dwelltime"];
   /*if(timeNum > 15){
     if(!query["session_id"]){
       query["session_id"] = state.env['ipaddress'] +"_"+ new Date(moment().format('YYYY-MM-DD HH:mm:00')).getTime();
     }
     isGetCompared = false;
     EXPORTS.requestHttpData(EXPORTS.constructhttpRequestBody(query, metrics,table), function(err, data) {
       if (err) {
         console.error(err);
         res.json({
           status: 500,
           content: {},
           msg: 'To get httpRequest data failed!'
         });
         return;
       }
       res.json({
         status: 200,
         msg:"task id:" + data["task_id"]
       });
     },req);
   }else {*/
  var mysql = require('mysql');
  var conn = mysql.createConnection({
    host: 'dashboard.cxleyzgw272j.us-east-1.rds.amazonaws.com',
    user: 'dashboard_w',
    password: '745e0390deb0abe4c46bbe3f13efa490',
    database:'dashboard',
    port: 3306
  });

  EXPORTS.requestOverallData(EXPORTS.constructArticleDailyOperateSql(query),conn, function(err, data) {
    if(err){
      console.error(err);
      res.json({
        status:500,
        content:{},
        msg:'TO get mysql data failed!'
      });
      return;
    }
    data.forEach(function(d){
      timezone = moment().tz("UTC").format('Z');
      d["timestamp"]  = moment(d["result"]["timestamp"]).format('YYYY-MM-DDTHH:mm') + timezone;
      delete d["result"]["timestamp"];
      delete d["result"]["ctr"];
    });
    derivedData = calcDerivedData(data,chart);

    var conn2 = mysql.createConnection({
      host: 'dashboard.cxleyzgw272j.us-east-1.rds.amazonaws.com',
      user: 'dashboard_w',
      password: '745e0390deb0abe4c46bbe3f13efa490',
      database:'dashboard',
      port: 3306
    });
    EXPORTS.requestOverallData(EXPORTS.constructArticleDailySumSql(query),conn2, function(err2, data2) {
      data2.forEach(function(d2){
        derivedData.forEach(function(d){
          if(d["result"]["click"]&&d["result"]["impression"]){
            d["result"]["ctr"] = (d["result"]["click"]*100/(d["result"]["impression"])).toFixed(2)+"%";
          }else{
            d["result"]["ctr"] = 0+"%";
          }
          if(d["result"]["click"]&&d["result"]["request"]){
            d["result"]["ctr_req"] = (d["result"]["click"]*100/(d["result"]["request"])).toFixed(2)+"%";
          }else{
            d["result"]["ctr_req"] = 0+"%";
          }
          if(d["result"]["avg_dwelltime"] && d["result"]["avg_dwelltime"] != 'Infinity'){
            d["result"]["avg_dwelltime"] = d["result"]["avg_dwelltime"].toFixed(2);
          }else{
            d["result"]["avg_dwelltime"] = 0;
          }

          d["result"]["request_all"] = d2["result"]["request_sum"];
          d["result"]["impression_all"] = d2["result"]["impression_sum"];

          if(d2["result"]["request_sum"]&&d["result"]["request"]){
            d["result"]["request_rate"] = (d["result"]["request"]/d2["result"]["request_sum"]).toFixed(4);
          }else{
            d["result"]["request_rate"] = 0;
          }
          if(d2["result"]["impression_sum"]&&d["result"]["impression"]){
            d["result"]["impression_rate"] = (d["result"]["impression"]/d2["result"]["impression_sum"]).toFixed(4);
          }else{
            d["result"]["impression_rate"] = 0;
          }
        });
      });
      res.json({
        status:200,
        content:derivedData,
        msg:'success!'
      });
    }, req);
  });
// }
}else if(dataSource.mysql4){
     var table = query.dataType,
      timeNum =  Math.floor(((new Date()).getTime() - (new Date(query.startTime)).getTime())/(24*60*60*1000)),
      metrics = ["sum(request) as request","sum(impression) as impression","sum(click) as click","sum(dwelltime) as dwelltime"];
  /* if(timeNum > 14){
     if(!query["session_id"]){
       query["session_id"] = state.env['ipaddress'] +"_"+ new Date(moment().format('YYYY-MM-DD HH:mm:00')).getTime();
     }
     isGetCompared = false;
     EXPORTS.requestHttpData(EXPORTS.constructhttpRequestBody(query, metrics,table), function(err, data) {
       if (err) {
         console.error(err);
         res.json({
           status: 500,
           content: {},
           msg: 'To get httpRequest data failed!'
         });
         return;
       }
       res.json({
         status: 200,
         msg:"task id:" + data["task_id"]
       });
     },req);
   }else {*/
        EXPORTS.requestMysqlData(EXPORTS.constructArticleDailySql(query),function(err, data) {
            if(err){
                console.error(err);
                res.json({
                    status:500,
                    content:{},
                    msg:'To get mysql data failed!'
                });
                return;
            }
            data.forEach(function(d){
                timezone = moment().tz("Asia/Shanghai").format('Z');
                d["timestamp"]  = moment(d["result"]["timestamp"]).format('YYYY-MM-DDTHH:mm') + timezone;
                delete d["result"]["timestamp"];
                delete d["result"]["ctr"];
            });
            derivedData = calcDerivedData(data,chart);
            derivedData.forEach(function(d){
                if(d["result"]["click"]&&d["result"]["impression"]){
                    d["result"]["ctr"] = (d["result"]["click"]*100/(d["result"]["impression"])).toFixed(2)+"%";
                }else{
                    d["result"]["ctr"] = 0+"%";
                }
                if(d["result"]["click"]&&d["result"]["request"]){
                    d["result"]["ctr_req"] = (d["result"]["click"]*100/(d["result"]["request"])).toFixed(2)+"%";
                }else{
                    d["result"]["ctr_req"] = 0+"%";
                }
                if(d["result"]["avg_dwelltime"] && d["result"]["avg_dwelltime"] != 'Infinity'){
                    d["result"]["avg_dwelltime"] = d["result"]["avg_dwelltime"].toFixed(2);
                }else{
                    d["result"]["avg_dwelltime"] = 0;
                }
            });
            res.json({
                status:200,
                content:derivedData,
                msg:'success!'
            });
        }, req);
//  }
 }else if(dataSource.mysql1){
        EXPORTS.requestMysqlData(EXPORTS.constructUserAge(query), function(err, data) {
            if(err){
                console.error(err);
                res.json({
                    status:500,
                    content:{},
                    msg:'To get mysql data failed!'
                });
                return;
            }
            data.forEach(function(d){
                timezone = moment().tz("Asia/Shanghai").format('Z');
                d["timestamp"]  = moment(d["result"]["timestamp"]).format('YYYY-MM-DDTHH:mm') + timezone;
                delete d["result"]["timestamp"];
            });
            derivedData = calcDerivedData(data,chart);
            var sumImp = 0;
            derivedData.forEach(function(result){
                sumImp += result["result"]["total_impression_user"];
            });
            derivedData.forEach(function(d){
                if(d["result"]["total_click"]&&d["result"]["total_impression"]){
                    d["result"]["ctr"] = (d["result"]["total_click"]*100/(d["result"]["total_impression"])).toFixed(2)+"%";
                }else{
                    d["result"]["ctr"] = 0+"%";
                }
                if(d["result"]["total_listpage_dwell_time"]&&d["result"]["total_impression_user"]){
                    d["result"]["listpage_imp_user"] = (d["result"]["total_listpage_dwell_time"]/(d["result"]["total_impression_user"])).toFixed(2);
                }else{
                    d["result"]["listpage_imp_user"] = 0;
                }
                if(d["result"]["total_impression_user"]){
                    d["result"]["impression_user_rate"] = (d["result"]["total_impression_user"]*100/sumImp).toFixed(2) + "%";
                }else{
                    d["result"]["impression_user_rate"] = 0+"%";
                }
                d["result"]["avg_detail_dwelltime_under_click"] = d["result"]["avg_detail_dwelltime_under_click"].toFixed(2);
                d["result"]["avg_click_under_impression_user"] = d["result"]["avg_click_under_impression_user"].toFixed(2);
                d["result"]["avg_click_under_click_user"] = d["result"]["avg_click_under_click_user"].toFixed(2);
                d["result"]["avg_impression_under_impression_user"] = d["result"]["avg_impression_under_impression_user"].toFixed(2);
                d["result"]["avg_request_under_request_user"] = d["result"]["avg_request_under_request_user"].toFixed(2);
            });
            res.json({
                status:200,
                content:derivedData,
                msg:'success!'
            });
        }, req);
 }else if (dataSource.mysql) {
      EXPORTS.requestMysqlData(EXPORTS.constructSql(query), function(err, data) {
        if(err){
          console.error(err);
          res.json({
            status:500,
            content:{},
            msg:'To get mysql data failed!'
          });
          return;
        }
        derivedData = calcDerivedData(data,chart);

        // hacky code below
        derivedData.forEach(function(data){
          if(data["result"]["publish_time"]) {
            data["result"]["publish_time"] = global.moment(new Date(data["result"]["publish_time"])*1000).format('YYYY-MM-DD HH:mm:ss');//将long型发布时间转化为DateTime格式
          }
        });
        res.json({
          status:200,
          content:derivedData,
          msg:'success!'
        });
     }, req);
}else  if (dataSource.httpRequest) {
      // Table format only request http Data

      var request = require("request")

      var url = "http://54.183.107.83:8080/t/?openid=test_openid&v=2&ns=s&format=json&c=shine"

      request({
          url: url,
          json: true
      }, function (error, response, body) {

          if (!error && response.statusCode === 200) {
              console.log(body) 
          }
      });
  }  else {
    res.json({
      status: 400,
      content: {},
      msg: '数据格式不合!'
    });
  }
});


// cc
router.post('/data/histogram/:chart', function(req, res) {
  var query = req.body || {},
    isGetCompared = req.query.type === 'compare',
    filter = query.filter || {},
    combinationFilterList,
    chart = req.params.chart,
    config = EXPORTS.getChartConf(chart),
    dataSource = EXPORTS.getChartDataSourceConf(chart),
    asyncFunc = {};

    //有filterMetric 的多次查询
    function queryMulFilterr(){
        if(dataSource.druid.filter_metrics){
            var ext_filter  =[];
            var ext_metrics = [];
            dataSource.druid.filter_metrics.forEach(function(e) {
                ext_metrics.push( e.keys.split(","));
                ext_filter.push(e.filter);
            }) ;
        }
    }

    function calcDerivedData(data, chartName) {
        // id to name map
        var idNameMap = EXPORTS.getChartMetricsIdNameMap(chartName),
        calculateMetrics = EXPORTS.getChartMetricsNeedCalculate(chartName);
        // 处理每个时间点httpRequest
        data.forEach(function(item) {
        var time = new Date(item.timestamp).getTime(),
            result = item && (item.result || item.event) || [],
            records = _.isArray(result) && result || [result];

        // 处理table里面的每一条记录
        records.forEach(function(record) {
            calculateMetrics.forEach(function(cm) {
                var evalState = '',
                dependency = cm.dependency || [],
                mid,
                mname,
                j;
                for (j = 0; j < dependency.length; j++) {
                  mid = dependency[j]; // metrics id
                  mname = idNameMap[mid].name; // metrics name (field names in druid or MySQL)
                  evalState += ('var ' + mid + ' = ' + record[mname] + ';');
                }
                evalState += 'expressR = ' + cm.expression + ';';

                try {
                  eval(evalState);
                  if (!isNaN(expressR)) {
                    record[cm.name] = expressR;
                  }else {
                    record[cm.name] = 0;
                  }
                }
                catch(exception) {
                  record[cm.name] = 0;
                }
            });// calculatedMetrics foreach
             if(record['article_id']){
                  record['id'] = record['article_id'];
              }
            // 处理href
            _.keys(record).forEach(function(rkey) {
              if (_.has(idNameMap[rkey],'href')) {
                var rvalue = record[rkey], hvalue = record[idNameMap[rkey].href];
                record[rkey] = '<a href="' + hvalue + '" target="_blank"'+ '  title ="'+rvalue+'" >' + rvalue + '</a>';
              }
             });
        });// records foreach

        });// data foreach

        return data;
    }

  function parseAndCalculateRes(resList, err, callback) {

    if (err) {
      console.error(err)
      callback(err);
      return;
    }

    var derivedRes = calcDerivedMetrics(resList, chart),
      pointCount = derivedRes.length,
      sumRes = {},
      idNameMap = EXPORTS.getChartMetricsIdNameMap(chart),
      metricIDs = _.keys(idNameMap);

    metricIDs.forEach(function(mid) {
      mname = idNameMap[mid].name;
      sumRes[mname] = 0;
    });
    // compute sum
    derivedRes.forEach(function(point){

      var values = point.result;
      metricIDs.forEach(function(mid){
        mname = idNameMap[mid].name;
        values[mname] = values[mname] || 0;
        sumRes[mname] += (values[mname] / pointCount);
      });
    });

    metricIDs.forEach(function(mid) {
      mname = idNameMap[mid].name;
      sumRes[mname] = sumRes[mname]>1000 && Math.round(sumRes[mname]) || sumRes[mname];
    });
    callback(null, {
      data: derivedRes,
      sum: sumRes
    });
  }

  function calcDerivedMetrics(resList, chartName) {
    // Use the first result set on the list as the base to get timestamps
    var res0 = resList && resList[0] || [],
      resLen = resList.length,
      resMap = {},
      i;
    var calculateMetrics = EXPORTS.getChartMetricsNeedCalculate(chartName),
      idNameMap = EXPORTS.getChartMetricsIdNameMap(chartName);

    // make a resMap[res-i][time] -> a list of metric values
    for (i = 0; i < resLen; i++) {
      resMap['res' + i] = {};
      (resList[i] || []).forEach(function(r) {
        resMap['res' + i][new Date(r.timestamp).getTime()] = r.result;
      });
    }

    res0.forEach(function(item) {
      var time = new Date(item.timestamp).getTime(),
        allMetrics = item && item.result || {};

      for (i = 1; i < resLen; i++) {
        _.extend(allMetrics, resMap['res' + i][time]);
      }

      calculateMetrics.forEach(function(cm) {
        var evalState = '',
          dependency = cm.dependency || [],
          mid,
          mname,
          j;

        for (j = 0; j < dependency.length; j++) {
          mid = dependency[j]; // metrics id
          mname = idNameMap[mid].name; // metrics name (field names in druid or MySQL)
          evalState += ('var ' + mid + ' = ' + allMetrics[mname] + ';');
        }
        evalState += 'expressR = ' + cm.expression + ';';

        try {
          eval(evalState);
          if (!isNaN(expressR)) {
            allMetrics[cm.name] = expressR;
          }else {
            allMetrics[cm.name] = 0;
          }
        }
        catch(exception) {
          allMetrics[cm.name] = 0;
        }

      });

    });
    return res0;
  }

    //多次查询信息
   function mulDruidRequest(q,callback){
       for(i in dataSource.length){
           EXPORTS.requestDruidData(EXPORTS.constructDruidPostBody(q), function(err, res) {
               parseAndCalculateRes([res], err, callback);
           });
       }
    }

  function requestData(q, callback) {
   if(dataSource.mysql_user_histogram){
       EXPORTS.requestMysqlData(EXPORTS.constructUserHistogramSql(query),function(err, data) {
            if(err){
                console.error(err);
                res.json({
                    status:500,
                    content:{},
                    msg:'To get mysql data failed!'
                });
                return;
            }
            data.forEach(function(d){
                timezone = moment().tz("Asia/Shanghai").format('Z');
                d["timestamp"]  = moment(d["result"]["timestamp"]).format('YYYY-MM-DDTHH:mm') + timezone;
                delete d["result"]["timestamp"];
            });
            derivedData = calcDerivedData(data,chart);
            res.json({
                status:200,
                content:derivedData,
                msg:'success!'
            });

        }, req);
   } else {
      EXPORTS.requestMysqlData(EXPORTS.constructSql(q), function(err, res) {
        parseAndCalculateRes([res], err, callback);
      }, req);
    }
  }
  query.dataType = chart;
  combinationFilterList = EXPORTS.getFilterCombinationList(filter);

  if (!isGetCompared) {
    combinationFilterList.forEach(function(e) {

      var newQuery = EXPORTS.objectCopyDeep(query),
        queryKey = '',
        i;
      if (_.isEmpty(e)) {

        queryKey = '*';
      } else {
        for (i in e) {
          queryKey += (i + ':' + e[i].values[0] + '\t');
        }
      }
      newQuery.filter = e;
      asyncFunc[queryKey] = function(callback) {
        requestData(newQuery, callback);
      };
    });
  } else {
    asyncFunc['yesterday'] = function(callback) {
      var newQuery = EXPORTS.objectCopyDeep(query);
      newQuery.startTime = EXPORTS.adjustTimeStrByDay(query.startTime, -1);
      newQuery.endTime = EXPORTS.adjustTimeStrByDay(query.endTime, -1);
      requestData(newQuery, callback);
    };
    asyncFunc['lastweek'] = function(callback) {
      var newQuery = EXPORTS.objectCopyDeep(query);
      newQuery.startTime = EXPORTS.adjustTimeStrByDay(query.startTime, -7);
      newQuery.endTime = EXPORTS.adjustTimeStrByDay(query.endTime, -7);
      requestData(newQuery, callback);
    };
  }

  async.parallel(asyncFunc, function(err, result) {
    if (err) {
      console.error(err);
      res.json({
        status: 500,
        content: {},
        msg: 'Get data failed!'
      });
      return;
    }

    res.json({
      status: 200,
      content: result,
      msg: 'success!'
    });
  });

});




module.exports = router;
