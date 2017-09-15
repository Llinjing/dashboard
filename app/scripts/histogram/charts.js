/*
  this module is for highcharts
*/

define('histogram/charts', function(exports) {
  'use strict';

  var utility = require('utility'),
    chartToolTipFormat,
    getMetricHighChartsConfig,
    getCompareDataAndDraw;


  chartToolTipFormat = function(context, tooltip, granularity, dataKeys, template) {
    var items = context.points || splat(context),
      dataKeysLength = dataKeys.length,
      dataBase = dataKeys && dataKeys[0],
      dataBaseValue,
      s;

    // build the header
    s = ['<span style="font-size: 10px">' + utility.dateToFormatStr(items[0].key, {
      granularity: granularity,
      dailyFormat: 'ddd, YYYY-MM-DD',
      hourlyFormat: 'ddd, YYYY-MM-DD HH:mm'
    }) + '</span><br/>'];

    // sort the values, descending
    items.sort(function (a, b) {
      if (a.series.name === dataBase) {
        dataBaseValue = a.y;
      }
      if (b.series.name === dataBase) {
        dataBaseValue = b.y;
      }
      return ((a.y > b.y) ? -1 : ((a.y < b.y) ? 1 : 0));
    });

    // build the values
    $.each(items, function (i, item) {
      var change,
        changeStr = '',
        type,
        series = item.series,
        value = item.y;

      if (dataKeysLength === 1) {
        if (series.name === dataBase) {
          changeStr += '';
        } else {
          change = utility.formatNumberPrecision((dataBaseValue - value) * 100 / value);
          type = (series.name === 'Yesterday') ? '环比昨日' : '同比上周';
          if (change > 0) {
            changeStr += '<span style="color:red"> (' + type + '增长' + change + '%)</span>';
          } else if (change < 0) {
            changeStr += '<span style="color:green"> (' + type + '下降' + (-change) + '%)</span>';
          } else {
            changeStr += ' 持平';
          }
        }
        //s.push(utility.formatLabelStr(utility.formatNumberPrecision(value).toLocaleString(), template) + '</b>'+ changeStr + '<br/>');
        s.push('<span style="color:' + series.color + '">' + series.name + '</span>: <b>' + utility.formatLabelStr(utility.formatNumberPrecision(value).toLocaleString(), template) + '</b>' + changeStr + '<br/>');
      } else {
        if (series.name === dataBase) {
          changeStr += ' (Base)';
        } else {
          change = utility.formatNumberPrecision((value - dataBaseValue) * 100 / dataBaseValue);
          if (change > 0) {
            changeStr += '<span style="color:red"> (↑' + change + '%)</span>';
          } else if (change < 0) {
            changeStr += '<span style="color:green"> (↓' + (-change) + '%)</span>';
          } else {
            changeStr += ' (=)';
          }
        }
        //s.push(utility.formatLabelStr(utility.formatNumberPrecision(value).toLocaleString(), template) + '</b>'+ changeStr + '<br/>');
        s.push('<span style="color:' + series.color + '">' + series.name + '</span>: <b>' + utility.formatLabelStr(utility.formatNumberPrecision(value).toLocaleString(), template) + '</b>' + changeStr + '<br/>');
      }
    });

    // footer
    s.push(tooltip.options.footerFormat || '');

    return s.join('');
  };

  getMetricHighChartsConfig = function(chartData,  options, chartContainerList, index) {
    var seriesData = [],
        categories = [],
//      mname = metric.name,
      id = options.dataUrl.substring(options.dataUrl.lastIndexOf("/")+1),
      template = "" ,
      title = "",
      subtitle =  '',
      granularity = options.granularity,
      //dataKey='group_id_v2',
      //reportConfig = EXPORTS.getChartConf(id),
      xaxis=options.xFieldName,
      yaxis=options.yFieldName;

      
   
    for (var i=0;i<chartData.length; i++) {
      //console.log("cc: "+chartData[i][xaxis] + chartData[i]["count"]);
        seriesData.push(chartData[i].result[yaxis]);
        categories.push(chartData[i].result[xaxis]);
    }
    
    var series =[ {name:xaxis,data:seriesData} ];

    //console.log("cc: "+JSON.stringify(series));
    return {
      chart: {
        type: 'column',
        zoomType: 'x'
      },
      //colors: ['#2f7ed8', '#910000', '#8bbc21', '#492970', '#f28f43', '#0d233a', '#77a1e5', '#c42525', '#a6c96a'],
      title: {
        text: title
      },
      subtitle: {
          text: subtitle,
          style: {
            color: '#333333',
            fontFamily : '微软雅黑,宋体'
          }
      },
      legend: {
         enabled:false
      },
      xAxis: {
        //type: 'category',
        title: {
          text : xaxis
        },
        categories:categories
        //tickInterval: 24 * 3600 * 1000,
        //tickAmount: 8,
        //min: utility.createLocalTime(options.startTime),
        //max: utility.createLocalTime(options.endTime)
      },
      yAxis: [{
        title: {
          text : yaxis
        }
      }],
      series: series
      //tooltip: {
       // formatter: function (tooltip) {
         // return chartToolTipFormat(this, tooltip, granularity, options.dataKeys, template);
        //},
        //shared: true
      //}
    };
  };

  getCompareDataAndDraw = function(options, chartContainerList) {
    var postBody = options.postBody,
      metricList = options.metrics || [];
    $.post(options.compareUrl || options.dataUrl + '?type=compare', postBody).done(function(res) {
      if (res.status === 200) {
        var rawData = res.content,
          addSeries;

        addSeries = _.map(metricList, function (metric) {
          var mname = metric.name,
            keys = [{
              name: 'yesterday',
              label: 'Yesterday'
            }, {
              name: 'lastweek',
              label: 'Last Week'
            }],
            series = [],
            formatData,
            key;

          keys.forEach(function(key) {
            formatData = [];
            (rawData[key.name].data || []).forEach(function(point) {
              var v = point.result[mname],
                delta = (key.name === 'yesterday') ? 24*3600*1000 : 7*24*3600*1000;
              formatData.push([
                new Date(point.timestamp).getTime() + delta,
                v === 'NaN' ? 0 : v
              ]);
            });
            series.push({
              name: key.label,
              visible: key.label == "Yesterday" ? true : false,
              data: formatData
            }); 
          });

          return series;
        });

        _.each(chartContainerList, function (chartNode, idx) {
          chartNode.highcharts().addSeries(addSeries[idx][0]);
          chartNode.highcharts().addSeries(addSeries[idx][1]);
        });

      } else {
        $.notify(res.msg, {
          className: '获取同比和环比数据失败！',
          autoHide: true
        });
      }
    }).fail(function(err) {
      $.notify('获取同比和环比数据失败！', {
        className: 'error',
        autoHide: true
      });
    });
  };

  exports.draw = function(options) {
    var resultChartsContainer = $('.chart-container'),
      metricList =  [{label:"test",name:"testname"}],
      chartContainerList,
      dataKeys = _.keys(options.data),
      chartData = options.data,
      chartConfigs,
      id = options.dataUrl.substring(options.dataUrl.lastIndexOf("/")+1),
      postBody = options.postBody;
    
    if(id==="index_user_histogram"){
        options['xFieldName'] = "num";
        options['yFieldName'] = "user_count";
    }
    
    options.dataKeys = dataKeys;
    resultChartsContainer.html('');
    //var chartBox = $('<div class="col-sm-12 chart-box"><div class="result-chart"></div></div>').appendTo(resultChartsContainer);
    //chartContainerList = $('.result-chart', chartBox);
    chartContainerList = _.map(metricList, function () {
      var chartBox = $('<div class="col-sm-12 chart-box"><div  class="result-chart"></div></div>').appendTo(resultChartsContainer);
      return $('.result-chart', chartBox);
    });    

    chartConfigs = getMetricHighChartsConfig(chartData, options, chartContainerList);
    //chartNode.highcharts(chartConfigs);
    _.each(chartContainerList, function (chartNode, idx) {
      chartNode.highcharts(chartConfigs);
    });


    //if (options.dataKeys.length === 1) {
    //  getCompareDataAndDraw(options, chartContainerList);
    //}

  };

});
