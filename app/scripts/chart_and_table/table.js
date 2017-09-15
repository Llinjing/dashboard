/*
  this module is for experiment table
*/
define('chart_and_table/table', function(exports) {
  'use strict';

  var utility = require('utility'),
    dataIndexByTime,
    formatDataForTable,
    getColumnConfig;

  dataIndexByTime = function(rawDataByTime, point, dataKey) {
    var r;

    if (!rawDataByTime[point.timestamp]) {
      rawDataByTime[point.timestamp] = {};
      rawDataByTime[point.timestamp].timestamp = point.timestamp;
    }
    if (!rawDataByTime[point.timestamp].dataKey) {
      rawDataByTime[point.timestamp].dataKey = [];
    }
    rawDataByTime[point.timestamp].dataKey.push(dataKey);
    for (r in point.result) {
      if (!rawDataByTime[point.timestamp][r]) {
        rawDataByTime[point.timestamp][r] = [];
      }
      rawDataByTime[point.timestamp][r].push(point.result[r]);
    }
  };

  formatDataForTable = function(rawData, dataKeys) {
    var rawDataByTime = {},
      formatData = [],
      r;

    dataKeys.forEach(function(dataKey) {
      // handle raw data
      rawData[dataKey].data.forEach(function(p) {
        dataIndexByTime(rawDataByTime, p, dataKey);
      });
      // handle summary data
      dataIndexByTime(rawDataByTime, {
        timestamp: 0,
        result: rawData[dataKey].sum
      }, dataKey);
    });

    for (r in rawDataByTime) {
      if (r !== '0') {
        formatData.push(rawDataByTime[r]);
      }
    }
    formatData.push(rawDataByTime['0']);
    formatData = formatData.reverse();

    return formatData;
  };

  getColumnConfig = function(options) {
    var metricList = options.metrics,
      dataKeyLength = options.dataKeys.length,
      granularity = options.granularity,
      tableColumns;

    tableColumns = [{
      data: 'timestamp',
      title: 'Timestamp',
      className: 'dt-center',
      render: function(data) {
        if (data) {
          return '<div class="timestamp">' + utility.dateToFormatStr(data, {
            granularity: granularity,
            dailyFormat: 'YYYY-MM-DD, ddd',
            hourlyFormat: 'YYYY-MM-DD HH:mm, ddd'
          }) + '</div>';
        } else {
          return '<div class="timestamp summary">AVERAGE</div>';
        }
      }
    }, {
      data: 'dataKey',
      title: 'Data Key',
      className: 'dt-center',
      render: function(data) {
        var renderHtml = '';
        data.forEach(function(d, index) {
          if (index) {
            renderHtml += ('<li class="exp expid">' + d + ' <span class="font-blue">(Compare)</span></li>');
          } else {
            if (dataKeyLength > 1) {
              renderHtml += ('<li class="base">' + d + ' <span class="font-blue">(Base)</span></li>');
            } else {
              renderHtml += ('<li class="base">' + d + '</li>');
            }
          }
        });
        return '<ul class="list-group">' + renderHtml + '</ul>';
      }
    }];

    metricList.forEach(function(m) {
      tableColumns.push({
        data: m.name,
        title: m.label || m.name,
        className: 'dt-center',
        render: function(data) {
          var renderHtml = '',
            data = data || [],
            base = data[0];
          data.forEach(function(d, index) {
            var change = utility.formatNumberPrecision((d - base) * 100 / base),
              localeString = utility.formatLabelStr((utility.formatNumberPrecision(d).toLocaleString()), m.template),
              changeStr;
            if (change > 0) {
              changeStr = '<p>' + localeString + '</p><p><strong class="font-red">↑' + change + '%</strong></p>';
            } else if (change < 0) {
              changeStr = '<p>' + localeString + '</p><p><strong class="font-green">↓' + (-change) + '%</strong></p>';
            } else if (index) {
              changeStr = '<p>' + localeString + '</p><p><strong>0%</strong></p>';
            } else {
              changeStr = localeString;
            }
            renderHtml += '<li class="' + (index ? 'exp' : 'base') + '">' + changeStr + '</li>'
          });

          return '<ul class="list-group">' + renderHtml + '</ul>';
        }
      });
    });

    return tableColumns;
  };

  exports.draw = function(options) {
    var tableData = formatDataForTable(options.data, options.dataKeys),
      tableColumns = getColumnConfig(options);

    $('.table-container').html('<table cellpadding="0" cellspacing="0" class="cell-border result-table"></table>' );
    $('.result-table').dataTable({
      data: tableData,
      pageLength: 100,
      ordering: false,
      columns: tableColumns
    });
  };

});
