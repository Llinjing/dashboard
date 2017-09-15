// 入口文件
var table = require('page_detail/table'),
  dataUrl = '/data/page_detail/' + window.settings.pageType;

require('common/detail_form').init({
  dataUrl: dataUrl,
  renderResult: function(drawOptions) {
    table.draw(drawOptions);
  }
});
