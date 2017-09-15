(function moduleDruidNodejs() {
    /*
     this module exports druid related methods
     */
    'use strict';

    var local = {

        _name: 'request.moduleDruidNodejs',

        _init: function () {
            EXPORTS.moduleInit(typeof module === 'object' ? module : null, local);
        },

        _getAggregationsCond: function(metrics) {
            var newAggregations = [],
                aggregationsList = {
                    total_service_user:{'type' : 'doubleSum',  'name' : 'total_service_user',   "fieldName": "total_service_user"},
                    tracking_total_user:{'type' : 'doubleMax',  'name' : 'tracking_total_user',   "fieldName": "tracking_total_user"},
                    retention_rate_1d: {'type': 'doubleSum', 'name': 'retention_rate_1d', 'fieldName': 'retention_rate_1d'},
                    compared_total_user_1d: {'type': 'doubleSum', 'name': 'compared_total_user_1d', 'fieldName': 'compared_total_user_1d'},
                    retention_rate_2d:{'type' : 'doubleSum',  'name' : 'retention_rate_2d', "fieldName": "retention_rate_2d"},
                    compared_total_user_2d:{ "type": "doubleSum","name": "compared_total_user_2d","fieldName": "compared_total_user_2d"},
                    retention_rate_3d:{'type' : 'doubleSum',  'name' : 'retention_rate_3d',   "fieldName": "retention_rate_3d"},
                    compared_total_user_3d: {'type': 'doubleSum', 'name': 'compared_total_user_3d', 'fieldName': 'compared_total_user_3d'},
                    retention_rate_4d: {'type': 'doubleSum', 'name': 'retention_rate_4d', 'fieldName': 'retention_rate_4d'},
                    compared_total_user_4d:{'type' : 'doubleSum',  'name' : 'compared_total_user_4d', "fieldName": "compared_total_user_4d"},
                    retention_rate_5d:{ "type": "doubleSum","name": "retention_rate_5d","fieldName": "retention_rate_5d"},
                    compared_total_user_5d:{'type' : 'doubleSum',  'name' : 'compared_total_user_5d',   "fieldName": "compared_total_user_5d"},
                    retention_rate_6d: {'type': 'doubleSum', 'name': 'retention_rate_6d', 'fieldName': 'retention_rate_6d'},
                    compared_total_user_6d: {'type': 'doubleSum', 'name': 'compared_total_user_6d', 'fieldName': 'compared_total_user_6d'},
                    retention_rate_7d:{'type' : 'doubleSum',  'name' : 'retention_rate_7d', "fieldName": "retention_rate_7d"},
                    compared_total_user_7d:{ "type": "doubleSum","name": "compared_total_user_7d","fieldName": "compared_total_user_7d"},
                    retention_rate_15d:{'type' : 'doubleSum',  'name' : 'retention_rate_15d',   "fieldName": "retention_rate_15d"},
                    compared_total_user_15d: {'type': 'doubleSum', 'name': 'compared_total_user_15d', 'fieldName': 'compared_total_user_15d'},
                    retention_rate_30d: {'type': 'doubleSum', 'name': 'retention_rate_30d', 'fieldName': 'retention_rate_30d'},
                    compared_total_user_30d:{'type' : 'doubleSum',  'name' : 'compared_total_user_30d', "fieldName": "compared_total_user_30d"},
                    total_request:{'type' : 'doubleSum',  'name' : 'total_request',   "fieldName": "total_request"},
                    total_request_user: {'type': 'doubleSum', 'name': 'total_request_user', 'fieldName': 'total_request_user'},
                    total_impression: {'type': 'doubleSum', 'name': 'total_impression', 'fieldName': 'total_impression'},
                    total_impression_user:{'type' : 'doubleSum',  'name' : 'total_impression_user', "fieldName": "total_impression_user"},
                    total_click:{ "type": "doubleSum","name": "total_click","fieldName": "total_click"},
                    total_click_user:{'type' : 'doubleSum',  'name' : 'total_click_user',   "fieldName": "total_click_user"},
                    total_detail_dwell_time: {'type': 'doubleSum', 'name': 'total_detail_dwell_time', 'fieldName': 'total_detail_dwell_time'},
                    total_detail_dwell_user: {'type': 'doubleSum', 'name': 'total_detail_dwell_user', 'fieldName': 'total_detail_dwell_user'},
                    total_listpage_dwelltime:{'type' : 'doubleSum',  'name' : 'total_listpage_dwelltime', "fieldName": "total_listpage_dwelltime"},
                    total_listpage_dwelltime_user:{ "type": "doubleSum","name": "total_listpage_dwelltime_user","fieldName": "total_listpage_dwelltime_user"},
                    avg_detail_dwelltime_under_impression:{"type": "doubleSum","name": "avg_detail_dwelltime_under_impression","fieldName": "avg_detail_dwelltime_under_impression"},
                    user_id: {'type': 'string', 'name': 'user_id', 'fieldName': 'user_id'},
                    article_id: {'type': 'string', 'name': 'article_id', 'fieldName': 'article_id'},
                    app: {'type': 'string', 'name': 'app', 'fieldName': 'app'},
                    click: {'type': 'doubleSum', 'name': 'click', 'fieldName': 'click'},
                    request: {'type': 'doubleSum', 'name': 'request', 'fieldName': 'request'},
                    impression: {'type': 'doubleSum', 'name': 'impression', 'fieldName': 'impression'},
                    dwelltime: {'type': 'doubleSum', 'name': 'dwelltime', 'fieldName': 'dwelltime'},
                    total_article_count:{'type' : 'count',  'name' : 'total_article_count',   "fieldNames": [ "article_id"] },
                    ge_2_percent_article_count: {'type': 'doubleSum', 'name': 'ge_2_percent_article_count', 'fieldName': 'ge_2_percent_article_count'},
                    ge_4_percent_article_count: {'type': 'doubleSum', 'name': 'ge_4_percent_article_count', 'fieldName': 'ge_4_percent_article_count'},
                    ge_8_percent_article_count: {'type': 'doubleSum', 'name': 'ge_8_percent_article_count', 'fieldName': 'ge_8_percent_article_count'},
                    max_gmp: {'type': 'doubleMax', 'name': 'max_gmp', 'fieldName': 'max_gmp'},
                    gmp_sum: {'type': 'doubleSum', 'name': 'gmp_sum', 'fieldName': 'gmp_sum'},
                    article_count: {'type': 'doubleSum', 'name': 'article_count', 'fieldName': 'article_count'},
		            total_click_user_count:{"type" : "filtered","filter" : {"type" : "selector","dimension" : "click_valid","value" :"true"},"aggregator" : {'type' : 'cardinality', 'name':'total_click_user_count', "fieldNames": [ "uid"], "byRow": false}},
                    total_request_user_count:{"type" : "filtered","filter" : {"type" : "selector","dimension" : "request_valid","value" :"true"},"aggregator" : {'type' : 'cardinality','name': 'total_request_user_count', "fieldNames": ["uid"], "byRow": false}},
                    total_impression_user_count:{"type" : "filtered","filter" : {"type" : "selector","dimension" : "impression_valid","value" :"true"},"aggregator" : {'type' : 'cardinality','name':'total_impression_user_count', "fieldNames": ["uid"], "byRow": false}},
                };

            metrics.forEach(function(m) {
                if (aggregationsList[m.name]) {
                    newAggregations.push(aggregationsList[m.name]);
                }
            });

            return newAggregations;
        },


        _getpostAggregationsCond: function(metrics) {
            var newPostAggregations = [],
                postAggregationsList = {
                    all_dwell_time_s: {
                        'type': 'javascript',
                        'name': 'all_dwell_time_s',
                        'fieldNames': [ 'article_dwell_time','list_dwell_time'],
                        'function':'function(list_dwell_time,article_dwell_time) { return (article_dwell_time+list_dwell_time)/1000; }'
                    },

                    article_dwell_time_s: {
                        'type': 'javascript',
                        'name': 'article_dwell_time_s',
                        'fieldNames': [ 'article_dwell_time'],
                        'function':'function(article_dwell_time) { return (article_dwell_time/1000)  ; }'
                    },
                    list_dwell_time_s: {
                        'type': 'javascript',
                        'name': 'list_dwell_time_s',
                        'fieldNames':[ 'list_dwell_time'],
                        'function':'function(list_dwell_time) { return (list_dwell_time/1000); }'
                    },

                    dpv: {
                        'type': 'javascript',
                        'name': 'dpv',
                        'fieldNames': ['dwelltime', 'view_count'],
                        'function': 'function(dwelltime, view_count) { return dwelltime / view_count; }'
                    },
                    dpc: {
                        'type': 'javascript',
                        'name': 'dpc',
                        'fieldNames': ['dwelltime', 'click_count'],
                        'function': 'function(dwelltime, click_count) { return dwelltime / click_count; }'
                    }
                };

            metrics.forEach(function(m) {
                if (postAggregationsList[m.name]) {
                    newPostAggregations.push(postAggregationsList[m.name]);
                }
            });

            return newPostAggregations;
        },


        _formatDruidDimensionFilterList: function(postBody, filter) {
            var i, filterFields = [], filter = filter || {};

            for (i in filter) {
                filterFields.push(local._getDruidDimensionFilterWithNotEqual(i, filter[i]));
            }
            if (filterFields.length) {
                postBody['filter'] = {
                    'type': 'and',
                    'fields': filterFields
                };
            }
        },

        _getDruidDimensionFilterWithNotEqual: function(name, queryObj) {
            var dimensionFilter = local._getDruidDimensionFilter(name, queryObj.values);

            return (queryObj['type'] === 'not') ? {
                'type': 'not',
                'field': dimensionFilter
            } : dimensionFilter;
        },

        _getDruidDimensionFilter: function(name, values) {
            var filterObj;
            if (!_.isArray(values)) {
                return undefined;
            }
            if (values.length >= 2) {
                filterObj = {
                    'type': 'or',
                    'fields': []
                };
                values.forEach(function(q) {
                    filterObj.fields.push({
                        'type': 'selector',
                        'dimension': name,
                        'value': q
                    });
                });
            } else {
                filterObj = {
                    'type': 'selector',
                    'dimension': name,
                    'value': values[0]
                };
            }
            return filterObj;
        },
        
        constructDruidPostBody: function(options, expid) {
            var
                startTime =  global.moment(new Date(options.startTime)).format('YYYY-MM-DDTHH:mm'),//YYYY-MM-DDTHH:00 精确到分 T表示后面的为时间
                endTime = global.moment(new Date(options.endTime)).format('YYYY-MM-DDTHH:mm'),
                granularity = options.granularity||'all',//如果未设置，就默认为组合全部类型
                queryType = options.queryType || 'timeseries',//默认查询时间类型
                dataSource = EXPORTS.getChartDataSourceConf(options.dataType)['druid']['source'],//查询json中定义的默认表
                groupBy = options.groupBy || [],//分组默认为空
                groupByLimit = options.limit || 3000,//查询条数默认为1000
                metrics = EXPORTS.getChartMetricsConf(options.dataType, 'druid'),//
                sortBy = options.sortBy || metrics[0].name,//排序
                filter = options.filter || {},//过滤  相当于where条件
                dataSourceType = options.dataSourceType,
                extFilter  = options.filter_metrics||{},
                groupByLength = groupBy.length,
                timezone,
                postBody,
                postBodyArr = [];
            if(dataSource.indexOf('shine_druid_retain_')>-1){
                dataSource = dataSourceType;
                if(dataSourceType !=dataSource){
                    dataSource = dataSourceType;
                }
            }
            if (state.env['timezone']) {
                switch (granularity) {
                    case 'day':
                        granularity =  {"type": "period", "period": "P1D", "timeZone": state.env['timezone']};
                        break;
                    case 'hour':
                        granularity =  {"type": "period", "period": "PT1H", "timeZone": state.env['timezone']};
                        break;
                    case 'minute':
                        granularity =  {"type": "period", "period": "PT1M", "timeZone": state.env['timezone']};
                        break;

                    case '10minute':
                        granularity =  {"type": "period", "period": "PT10M", "timeZone": state.env['timezone']};
                        break;
                }
                timezone = moment().tz(state.env['timezone']).format('Z');

                startTime += timezone;
                endTime += timezone;
            }

            postBody = {
                'dataSource': dataSource,
                'granularity': granularity,
                'aggregations': local._getAggregationsCond(metrics),
                'postAggregations' : local._getpostAggregationsCond(metrics),
                'intervals': startTime + '/' + endTime
            };

            // Filter
            local._formatDruidDimensionFilterList(postBody, filter);


            // Groupby
            if (groupByLength === 0) {
                queryType = 'timeseries';
            } else if (groupByLength === 1) {
                queryType = 'topN';
                postBody.dimension = groupBy[0];
                postBody.metric = sortBy;
                postBody.threshold = groupByLimit;
            } else {
                queryType = 'groupBy';
                postBody.dimensions = groupBy;
                postBody.limitSpec = {
                    'type': 'default',
                    'limit': groupByLimit,
                    //'columns': [sortBy]
                    'cloumns':[{
                        "dimension" :sortBy,
                        'direction':"descending"
                    }]
                };
                postBody.context = {
                    'timeout':240000,
                    'priority':0
                }
            }
            postBody.queryType = queryType;
            postBodyArr.push(postBody);
            return postBody;
        },


      _getDruidFormatter:function(postJson,data){
      var druidFormatter = new Array();
      if(data&&data[0]&&data[0].result instanceof  Array){
          data.forEach(function(mdata){
            var  timestamp = mdata.timestamp;
            var result = mdata.result;
            result.forEach(function(mresult){
              var keyValueFormatter = new Object(),
                  keyDruid = new Array(),
                  valueDruid = new Object();
                  keyDruid.push("timestamp");
                  valueDruid['timestamp'] = timestamp;
                  if(postJson.dimension){
                      keyDruid.push(postJson.dimension);
                    } else if(postJson.dimensions){
                      postJson.dimensions.forEach(function(mgroupby) {
                        keyDruid.push(mgroupby);
                      });
                  }
                  for(var key in mresult){
                    valueDruid[key]=mresult[key];
                  }
                  keyValueFormatter.key = keyDruid;
                  keyValueFormatter.value = valueDruid;
                  druidFormatter.push(keyValueFormatter);
            });
          });
      }else{
        data.forEach(function(mdata){
          var keyValueFormatter = new Object(),
              keyDruid = new Array(),
              valueDruid = new Object();
          if(mdata.timestamp||timestamp){
            keyDruid.push("timestamp");
            valueDruid["timestamp"] = mdata.timestamp ;
          }
          if(postJson.dimension){
            keyDruid.push(postJson.dimension);
          } else if(postJson.dimensions){
            postJson.dimensions.forEach(function(mgroupby) {
              keyDruid.push(mgroupby);
            });
          }
          var result = mdata.event || mdata.result|| timestamp&&mdata;
          for(var key in result){
            valueDruid[key]=result[key];
          }
          keyValueFormatter.key = keyDruid;
          keyValueFormatter.value = valueDruid;
          druidFormatter.push(keyValueFormatter);
        });
      }
      return druidFormatter;
    },


     requestDruidData: function(data, callback) {
            var postBody = JSON.stringify(data, null, 4);
            console.log('Druid Request:\n' + postBody);

            EXPORTS.requestNodejs({
                url: state.env['druid-url'],
                method: 'POST',
                data: postBody
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
