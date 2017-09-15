var express = require('express');
var router = express.Router();

//处理地址栏未指定访问页面的请求
router.get('/', function(req, res) {
  //跳转到
  res.redirect('/report/index');
});

//处理地址栏具体指定访问的页面
router.get('/report/:id', function(req, res) {
  console.log("09087656790:" + req.params.id + "##");
  var id = req.params.id,
    chartConfig = EXPORTS.getChartConf(id),
    chartType;

  if (!chartConfig) {
    res.status(404).send('This chart is not ready now~~~');
  }
  chartType = chartConfig['type'] || 'chart_and_table';
  res.render(chartType + '/index', EXPORTS.assetAddCssJs({
    asset: chartType,
    req: req,
    pageType: id
  }));
});

module.exports = router;
