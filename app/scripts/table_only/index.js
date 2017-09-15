// 入口文件
var table = require('table_only/table'),
  dataUrl = '/data/table_only/' + window.settings.pageType;

require('common/form').init({
  dataUrl: dataUrl,
  renderResult: function(drawOptions) {
    table.draw(drawOptions);
  }
});
