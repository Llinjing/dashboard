/*
  this module is for experiment table
*/
define('histogram/table', function(exports) {
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
     var formatData = [];
    rawData.forEach(function(timePoint) {
      var result = timePoint.result || timePoint.event,
        timestamp = timePoint.timestamp;

      if (_.isArray(result)) {
        result.forEach(function(r) {
          r.timestamp = timestamp;
          formatData.push(r);
        });
      } else {
        result.timestamp = timestamp;
        formatData.push(result);
      }
    });
    return formatData;  


  };

  getColumnConfig = function(options) {
      var metricList = options.metrics,
      groupByList = options.groupBy,
      granularity = options.granularity,
      tableColumns = [];

    if (granularity !== 'all') {
      tableColumns.push({
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
      });
    }

    if (groupByList) {
      groupByList.forEach(function(gb) {
        tableColumns.push({
          data: gb,
          title: $('.groupbyForm input[value="' + gb + '"]').parent().text().trim(),
          className: 'dt-center ' + gb
        });
      });
    }

    metricList.forEach(function(m) {
      tableColumns.push({
        data: m.name,
        title: m.label || m.name,
        className: 'dt-center',
        render: function(data) {
          if("title"){
              return data;
          }else{
              return utility.formatLabelStr((utility.formatNumberPrecision(data).toLocaleString()), m.template);
          }
        }
      });
    });

    return tableColumns;

  };

  exports.draw = function(options) {
    //var tableData = formatDataForTable(options.data, options.dataKeys),
     // tableColumns = getColumnConfig(options);

    //$('.table-container').html('<table cellpadding="0" cellspacing="0" class="cell-border result-table"></table>' );
    //$('.result-table').dataTable({
    //  data: tableData,
    //  pageLength: 100,
    //  ordering: false,
    //  columns: tableColumns
    //});
    
    var tableData = formatDataForTable(options.data, options.expids),
      tableColumns = getColumnConfig(options),
      sortColumn = (options.granularity !== 'all') ? 'timestamp' : (options.metrics && options.metrics[0] && options.metrics[0].name),
      findIndex = _.findIndex(tableColumns, function(t) {return t.data === sortColumn;}),
      sortedIndex = (findIndex === -1) ? 0 : findIndex;

    if (!options.metrics || options.metrics.length === 0) {
      return;
    }

    $('.table-container').html('<table cellpadding="0" cellspacing="0" class="cell-border result-table"><tr><td></td></tr></table>' );
    $('.result-table').dataTable({
      data: tableData,
      pageLength: 100,
      ordering: true,
      order: [[sortedIndex, 'desc']],
      columns: tableColumns
    });


  };

});
