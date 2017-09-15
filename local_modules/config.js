(function moduleConfigNodejs() {
  /*
    this module exports config related methods
  */
  'use strict';
  var config = require('../conf/' + state.env['conf-file']);

  var local = {

    _name: 'config.moduleConfigNodejs',

    _init: function () {
      EXPORTS.moduleInit(typeof module === 'object' ? module : null, local);
      local._constructChartConfig();
    },

    _constructChartConfig: function() {
      var chart,
        i, j;

      for (i in config.charts) {
        if (i === 'shared') {
          continue;
        }
        chart = config.charts[i];

        //修改dimensions与metrics
        ['dimensions', 'metrics'].forEach(function(j) {
          chart[j]['private'] = chart[j]['private'] || {};
          if (chart[j]['useShared']) {
            if (chart[j]['shared']) {
              chart[j]['private'] = local._mergeConfig(config.charts['shared'][j], chart[j]['private'], chart[j]['shared']);
            } else {
              chart[j]['private'] = local._mergeConfig(config.charts['shared'][j], chart[j]['private']);
            } 
          }
          chart[j + '_format'] = local._formatObj2Array(chart[j]['private']);
        });

        if (chart.groupbys) {
          // merge group values into metrics.private
          chart['metrics']['private'] = local._mergeConfig(config.charts['shared']['metrics'], chart['metrics']['private'], chart['groupbys']['values']);
          // 生成chart['groupby_format']
          chart['groupbys_format'] = [];
          chart.groupbys.values.forEach(function(gb) {
            // 取private dimension中的定义，或者shared metrics中的定义
            var gbf = EXPORTS.objectCopyDeep(chart['dimensions']['private'][gb] || config.charts['shared']['metrics'][gb]);
            // 设置default
            if (chart.groupbys["default"] === gb) {
              gbf['groupby_checked'] = true;
            }
            chart['groupbys_format'].push(gbf);
            // 处理groupby的特殊dependency：href。这个地方写的比较hacky，以后改吧。@zl
            if (gbf.href) { // 如果某个groupby需要href另一个metric，要把这个metrics的属性也取出来
              chart['metrics']['private'] = local._mergeConfig(config.charts['shared']['metrics'],
                chart['metrics']['private'], [gbf.href]);
            }
          });
        }

        if (chart['dataSource']) {
          chart['dataSource_map'] = {};

          chart['dataSource'].forEach(function(d) {
            chart['dataSource_map'][d.type] = d;
              //针对多条 druid 查询出来
          });
        }
      }
    },

    _mergeConfig: function(configuration, toMerge, shared) {
      var i, v, conf = EXPORTS.objectCopyDeep(configuration);

      if (shared) {
        conf = _.pick(conf, shared);
      }

      for (i in toMerge) {
        if (!conf[i]) {
          conf[i] = toMerge[i];
        } else {
          for (v in toMerge[i]) {
            conf[i][v] = toMerge[i][v];
          }
        }
      }

      return conf;
    },

    _formatObj2Array: function(obj) {
      var arrayRes = [], i;
      for (i in obj) {
        arrayRes.push(obj[i]);
      }

      return arrayRes;
    },

    //invenoaboard.json/chart/page_detail
    getChartDimensionsConf: function(name) {
      return config.charts[name] && config.charts[name]['dimensions_format'] || [];
    },
     
    getChartDimensionsConfOrigin:function(name){
          return config.charts[name] && config.charts[name]['dimensions']['private'] || [];
    },

    getChartMetricsConf: function(name, type) {
      var chartConfig = config.charts[name] || {},
        metricsAll = chartConfig['metrics_format'] || [],
        metricsMap = chartConfig['metrics']['private'] || {},
        dataSource = local.getChartDataSourceConf(name) || {},
        dataSourceType = dataSource[type] || {},
        chartMetrics = [],
        sourceFilterMetrics = [],//有过滤的数据信息
        sourceMetrics = [];

    //  console.log(['dataSourceType.metrics is *********** ',dataSourceType.metrics]);
      if (dataSourceType.metrics) {
        dataSourceType.metrics.forEach(function(m) {
          sourceMetrics.push(metricsMap[m] && metricsMap[m].name || m)
        });
      }
        //s2016年2月2日11:26:24  sxy add
        if(dataSource.filter_metrics){
            dataSource.filter_metrics.forEach(function(m) {
                sourceFilterMetrics.push(metricsMap[m] && metricsMap[m].name || m)
            });
        }
      metricsAll.forEach(function(m) {
          if (!sourceMetrics.length || _.indexOf(sourceMetrics, m.name) >= 0) {
              chartMetrics.push(m);
          }
      });
      //  console.log(['metricsAll is *********** ',metricsAll]);
      return chartMetrics;
    },

    getChartMetricsIdNameMap: function(name) {
      var chartConfig = config.charts[name] || {},
        metricsMap = chartConfig['metrics']['private'] || {};

      return metricsMap;
    },

    getChartDimensionIdNameMap: function(name) {
      var chartConfig = config.charts[name] || {},
        dimensionsMap = chartConfig['dimensions']['private'] || {};

      return dimensionsMap;
    },

    getChartMetricsNameIdMap: function(name) {
      var metricsMap = local.getChartMetricsIdNameMap(name),
        nameIdMap = {},
        id;

      for (id in metricsMap) {
        nameIdMap[metricsMap[id].name] = id;
      }

      return nameIdMap;
    },



    getChartMetricsNeedCalculate: function(name) {
      var metricsAll = config.charts[name] && config.charts[name]['metrics_format'] || [],
        dataSource = config.charts[name]['dataSource'] || [],
        metrics = metricsAll,
        nameIdMap = EXPORTS.getChartMetricsNameIdMap(name);

        dataSource.forEach(function(ds) {
        metrics = _.reject(metrics, function(mmm) {
          return _.indexOf(ds.metrics, nameIdMap[mmm.name]) >= 0;
        })
      });

      return metrics;
    },

    getChartGroupbyConf: function(name) {
      return config.charts[name] && config.charts[name]['groupbys_format'] || [];
    },

    getChartDataSourceConf: function(name) {
      return config.charts[name] && config.charts[name]['dataSource_map'];
    },

    getChartConf: function(name) {
      return config.charts[name];
    },
//     getChartConfExtFilterType:function(name){//多次查询filter map 在datasource 下的
//          return config.charts[name] && config.charts[name]['filter_map'];
//      },
    getChartConfType:function(name){
      return config.charts[name] && config.charts[name]['type'];
    },
    getConf: function() {
      return config;
    }

  };

  local._init();
}());
