/*
  this module provide utility functions
*/
define('utility', function(exports) {
  'use strict';

  exports.dateToFormatStr = function(date, options) {
    var dailyFormat = options && options.dailyFormat || 'MM/DD',
      hourlyFormat = options && options.hourlyFormat || 'MM/DD HH:mm',
      newDate = window.settings.timezone ? global.moment(new Date(date)).tz(window.settings.timezone) : global.moment(new Date(date)).utc(),
      isDaily = options && (options.granularity === 'day');

    return isDaily ? newDate.format(dailyFormat) : newDate.format(hourlyFormat);
  };

  exports.formatNumberStr = function(num) {
    return num.toString().replace(/(\d)(?=(\d{3})+(?!\d))/g, '$1,')
  };

  exports.formatNumberPrecision = function(number, precision) {
    var adjust = Math.pow(10, precision ||3);
    return Math.round(number * adjust) / adjust;
  };

  exports.createUtcTime = function(str) {
    return new Date(global.moment(new Date(str)).format('YYYY-MM-DDTHH:00')).getTime();
  };

  exports.createLocalTime = function(str) {
    return window.settings.timezone ? (new Date(global.moment(new Date(str)).tz(window.settings.timezone)).getTime()) : (new Date(global.moment(new Date(str)).format('YYYY-MM-DDTHH:00')).getTime());
  };

  exports.formatLabelStr = function(string, template) {
    if (template && typeof(template) === 'string') {
      return template.replace('%s', string);
    } else {
      return string;
    }
  };

    exports.getLocalTime = function(nS) {//格式化时间信息 时间戳 ---时间格式信息
        return new Date(parseInt(nS) * 1000).toLocaleString().replace(/年|月/g, "-").replace(/日/g, " ");
    };

});
