// 入口文件
var table = require('histogram/table'),
  chart = require('histogram/charts'),
  dataUrl = '/data/histogram/' + window.settings.pageType;

require('common/form').init({
  dataUrl: dataUrl,
  renderResult: function(drawOptions) {
    drawOptions.dataUrl = dataUrl;
    
    //用户数取整
    for(i in drawOptions['data']){
        for(j in drawOptions['data'][i]["data"]){
            drawOptions['data'][i]["data"][j]['result']["total_click_user_count"] = Math.round(drawOptions['data'][i]["data"][j]['result']["total_click_user_count"]);
            drawOptions['data'][i]["data"][j]['result']["total_impression_user_count"] = Math.round(drawOptions['data'][i]["data"][j]['result']["total_impression_user_count"]);
            drawOptions['data'][i]["data"][j]['result']["total_request_user_count"] = Math.round(drawOptions['data'][i]["data"][j]['result']["total_request_user_count"]);
            drawOptions['data'][i]["data"][j]['result']["total_dwelltime_user_count"] = Math.round(drawOptions['data'][i]["data"][j]['result']["total_dwelltime_user_count"]);
        }
    }

    chart.draw(drawOptions);
    table.draw(drawOptions);
  }

});
