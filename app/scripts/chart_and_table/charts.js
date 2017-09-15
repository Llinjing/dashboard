/*
  this module is for highcharts
*/

define('chart_and_table/charts', function(exports) {
  'use strict';

  var utility = require('utility'),
    formatDataForCharts,
    chartToolTipFormat,
    getMetricHighChartsConfig,
    getCompareDataAndDraw;

  formatDataForCharts = function(rawData, metrics, dataKeys) {
    var dataKey,
      formatData = {};

    metrics.forEach(function(metric) {
      var mname = metric.name;
      formatData[mname] = {};
      dataKeys.forEach(function(dataKey) {
        formatData[mname][dataKey] = [];
        (rawData[dataKey].data || []).forEach(function(point) {
          var v = point.result[mname];
          formatData[mname][dataKey].push([
            new Date(point.timestamp).getTime(),
            v === 'NaN' ? 0 : v
          ]);
        });
      });
    });

    return formatData;
  };

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
        var arr = series.name.indexOf("\t") ? series.name.split("\t") : series.name;
        var se = [];
        for(var i = 0; i < arr.length; i++) {
            if(arr[i].indexOf("all") == -1){
               se.push(arr[i]);
            }
        }
        se.join("\t");
        s.push('<span style="color:' + series.color + '">' + se + '</span>: <b>' + utility.formatLabelStr(utility.formatNumberPrecision(value).toLocaleString(), template) + '</b>' + changeStr + '<br/>');
        //s.push(utility.formatLabelStr(utility.formatNumberPrecision(value).toLocaleString(), template) + '</b>'+ changeStr + '<br/>');
        //s.push('<span style="color:' + series.color + '">' + series.name + '</span>: <b>' + utility.formatLabelStr(utility.formatNumberPrecision(value).toLocaleString(), template) + '</b>' + changeStr + '<br/>');
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
        var arr = series.name.indexOf("\t") ? series.name.split("\t") : series.name;
        var se = [];
        for(var i = 0; i < arr.length; i++) {
            if(arr[i].indexOf("all") == -1){
               se.push(arr[i]);
            }
        }
        se.join("\t");
        s.push('<span style="color:' + series.color + '">' + se + '</span>: <b>' + utility.formatLabelStr(utility.formatNumberPrecision(value).toLocaleString(), template) + '</b>' + changeStr + '<br/>');
        //s.push(utility.formatLabelStr(utility.formatNumberPrecision(value).toLocaleString(), template) + '</b>'+ changeStr + '<br/>');
        //s.push('<span style="color:' + series.color + '">' + series.name + '</span>: <b>' + utility.formatLabelStr(utility.formatNumberPrecision(value).toLocaleString(), template) + '</b>' + changeStr + '<br/>');
      }
    });

    // footer
    s.push(tooltip.options.footerFormat || '');

    return s.join('');
  };

  getMetricHighChartsConfig = function(chartData, metric, options, chartContainerList, index) {
    var series = [],
      mname = metric.name,
      template = metric.template,
      title = (metric.label || metric.name) + (template ? (' (' + utility.formatLabelStr('', template) + ')') : ''),
      subtitle = metric.subtitle || '',
      granularity = options.granularity,
      chart_type = options.chart_type,
      dataKey;

    for (dataKey in chartData[mname]) {
      series.push({
        name: dataKey,
        data: chartData[mname][dataKey]
      });
    }

    return {
      chart: {
        type: chart_type,
        zoomType: 'x'
      },
      subtitle: {
          text: subtitle,
          align:"left",
          style: {
            color: '#333333',
            'font-family' : '微软雅黑,宋体'
          }
      },
      colors: ['#2f7ed8', '#910000', '#8bbc21', '#492970', '#f28f43', '#0d233a', '#77a1e5', '#c42525', '#a6c96a'],
      title: {
        text: title,
        align:"left"
      },
      credits: false,
      legend: {
        width:200,
        maxHeight:100,
        enabled:true,
        backgroundColor: 'white',
        borderColor: '#ddd',
        borderWidth: 1,
        borderRadius: 4,
        floating: true,
        align: 'right',
        verticalAlign: 'top',
        layout: 'vertical',
        y: -10,
        x: -10,
        style: {
          zIndex: 99
        }
      },
      plotOptions: {
        series: {
          //connectNulls: false
          events: {
            legendItemClick: function () {
              var visible = this.visible,
                name = this.name;
 
              _.each(chartContainerList, function (chartNode, idx) {
                var series = chartNode.highcharts().series;
                series.forEach(function(c) {
                  if ((idx !== index) && (c.name === name)) {
                    visible ? c.hide() : c.show();
                  }
                });
              });
            }
          }
        }
      },
      xAxis: {
        type: 'datetime',
        labels: {
          formatter: function () {
            return utility.dateToFormatStr(this.value, {
              granularity: 'hour'
            });
          },
          step: 1,
          style: {
            fontSize: '0.75em'
          }
        },
        //tickInterval: 24 * 3600 * 1000,
        tickAmount: 8,
        min: utility.createLocalTime(options.startTime),
        max: utility.createLocalTime(options.endTime)
      },
      yAxis: [{
        title: {
          text : title
        },
        labels: {
          zIndex: 6
        },
        startOnTick: false
      }],
      series: series,
      tooltip: {
        formatter: function (tooltip) {
          return chartToolTipFormat(this, tooltip, granularity, options.dataKeys, template);
        },
        shared: true
      }
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
      metricList = options.metrics || [],
      chartContainerList,
      dataKeys = _.keys(options.data),
      chartData = formatDataForCharts(options.data, metricList, dataKeys),
      chartConfigs,
      postBody = options.postBody;

    options.dataKeys = dataKeys;
    resultChartsContainer.html('');
    chartContainerList = _.map(metricList, function () {
      var chartBox = $('<div class="col-sm-6 chart-box"><div class="result-chart"></div></div>').appendTo(resultChartsContainer);
      return $('.result-chart', chartBox);
    });
    chartConfigs = _.map(metricList, function (metric, index) {
      return getMetricHighChartsConfig(chartData, metric, options, chartContainerList, index);
    });
    _.each(chartContainerList, function (chartNode, idx) {
      chartNode.highcharts(chartConfigs[idx]);
    });

    if (options.dataKeys.length === 1) {
      getCompareDataAndDraw(options, chartContainerList);
    }

  };

});
