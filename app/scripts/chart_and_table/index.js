// 入口文件
var table = require('chart_and_table/table'),
  chart = require('chart_and_table/charts'),
  dataUrl = '/data/chart_and_table/' + window.settings.pageType;

require('common/form').init({
  dataUrl: dataUrl,
  renderResult: function(drawOptions) {
    drawOptions.dataUrl = dataUrl;
     
    //设置page 图表类型
    var chart_type='/data/chart_and_table/index';

    switch(dataUrl){
      case '/data/chart_and_table/index_column':
        chart_type='column';
        break;
      default:
        chart_type= 'spline';
        break;
    }
    drawOptions.chart_type = chart_type;    

    //用户数取整
    //for(i in drawOptions['data']){
    //    for(j in drawOptions['data'][i]["data"]){
    //        drawOptions['data'][i]["data"][j]['result']["total_click_user_count"] = Math.round(drawOptions['data'][i]["data"][j]['result']["total_click_user_count"]);
    //        drawOptions['data'][i]["data"][j]['result']["total_impression_user_count"] = Math.round(drawOptions['data'][i]["data"][j]['result']["total_impression_user_count"]);
    //        drawOptions['data'][i]["data"][j]['result']["total_request_user_count"] = Math.round(drawOptions['data'][i]["data"][j]['result']["total_request_user_count"]);
    //        drawOptions['data'][i]["data"][j]['result']["total_dwelltime_user_count"] = Math.round(drawOptions['data'][i]["data"][j]['result']["total_dwelltime_user_count"]);
    //    }
   // }
    for(i in drawOptions['data']){
      for(j in drawOptions['data'][i]["data"]){
         if(drawOptions['data'][i]["data"][j]['result']["total_click_user_count"]){
            drawOptions['data'][i]["data"][j]['result']["total_click_user_count"] = Math.round(drawOptions['data'][i]["data"][j]['result']["total_click_user_count"]);
         }
         if(drawOptions['data'][i]["data"][j]['result']["total_impression_user_count"]){
            drawOptions['data'][i]["data"][j]['result']["total_impression_user_count"] = Math.round(drawOptions['data'][i]["data"][j]['result']["total_impression_user_count"]);
         }
         if(drawOptions['data'][i]["data"][j]['result']["total_request_user_count"]){
            drawOptions['data'][i]["data"][j]['result']["total_request_user_count"] = Math.round(drawOptions['data'][i]["data"][j]['result']["total_request_user_count"]);
         }
         if(drawOptions['data'][i]["data"][j]['result']["total_dwelltime_user_count"]){
            drawOptions['data'][i]["data"][j]['result']["total_dwelltime_user_count"] = Math.round(drawOptions['data'][i]["data"][j]['result']["total_dwelltime_user_count"]);
         }
      }
    }


    chart.draw(drawOptions);
    table.draw(drawOptions);
  }

});
