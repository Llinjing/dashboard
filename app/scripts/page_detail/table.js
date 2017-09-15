/*
  this module is for details table
*/
define('page_detail/table', function(exports) {
  'use strict';
  var utility = require('utility'),
    dataIndexByTime,
    formatDataForTable,
    getColumnConfig;

  formatDataForTable = function(rawData) {
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

    metricList.forEach(function(m) {
      tableColumns.push({
        data: m.name,
        title: m.label || m.name,
        className: 'dt-center',
        render: function(data) {
          return utility.formatLabelStr((utility.formatNumberPrecision(data).toLocaleString()), m.template);
        }
      });
    });
    return tableColumns;
  };



    exports.draw = function(options) {
      var tableData = options.data,
          tableColumns = getColumnConfig(options),
          sortColumn = (options.granularity !== 'all') ? 'timestamp' : (options.metrics && options.metrics[0] && options.metrics[0].name),
          findIndex = _.findIndex(tableColumns, function (t) {
              return t.data === sortColumn;
          }),
          sortedIndex = (findIndex === -1) ? 0 : findIndex;

      if (!options.metrics || options.metrics.length === 0) {
          return;
      }
      var gender = tableData['gender']==2?"女":"男";
      var age = tableData['age'];
      var city = '';//城市 兴趣 机器型号---P0
      var registered_time = '', last_visit_time = '';//注册时间 最近访问时间 ---P1
      //个人兴趣标签 发布过的标签 点赞的标签 点击过的标签 ---P2
      var personal_interest = '', released_video_tags_str='',released_video_tags = [], liked_video_tags = [], clicked_video_tags = [];
      //总发布量 被点赞数  访客数  被favor数(粉丝数量) 手机型号 角色 birth date -P3
      var released_video_count= '', accumulated_like_count= '',
          accumulated_personalpage_visit_count= '', accumulated_fans_count= '',
          device_model= '', role = '', birth_date= '';


      var personalTags = tableData['tags'];
      for (var i = 0; i < personalTags.length; i++) {
          var version = personalTags[i].version;
          var tag = personalTags[i].tag;

          if (version.indexOf("Geographic_Register") > -1) {
              city = tag;
          } else if (version.indexOf("personal_interest") > -1) {
              personal_interest = tag;
          } else if (version.indexOf("device_model") > -1) {
              device_model = tag;
          }
      }

      var weighted_tags = tableData['weighted_tags'];
      for (var i = 0; i < weighted_tags.length; i++) {
          //personal_basicInfo 个人基础信息
          var weighted_tags_verrion = weighted_tags[i].version;
          var weighted_tags_info = weighted_tags[i].tag;
          if (weighted_tags_verrion.indexOf("personal_basicInfo") > -1) {
              for (var j = 0; j < weighted_tags_info.length; j++) {
                  var personal_basicInfo_tag = weighted_tags_info[j].tag;
                  var personal_basicInfo = weighted_tags_info[j].weight;
                  if (personal_basicInfo_tag.indexOf("registered_time") > -1) {
                      registered_time = utility.getLocalTime(personal_basicInfo);
                  } else if (personal_basicInfo_tag.indexOf("last_visit_time") > -1) {
                      last_visit_time =  utility.getLocalTime(personal_basicInfo);
                  } else if (personal_basicInfo_tag.indexOf("released_video_count") > -1) {
                      released_video_count = personal_basicInfo;
                  } else if (personal_basicInfo_tag.indexOf("accumulated_like_count") > -1) {
                      accumulated_like_count = personal_basicInfo;
                  } else if (personal_basicInfo_tag.indexOf("accumulated_personalpage_visit_count") > -1) {
                      accumulated_personalpage_visit_count = personal_basicInfo;
                  } else if (personal_basicInfo_tag.indexOf("accumulated_fans_count") > -1) {
                      accumulated_fans_count = personal_basicInfo;
                  } else if (personal_basicInfo_tag.indexOf("role") > -1) {

                      switch (personal_basicInfo) {
                          case 0:
                              role = '普通用户';
                              break;
                          case 1:
                              role = '兼职';
                              break;
                          case 2:
                              role = '网红';
                              break;
                          case 3:
                              role = '名人';
                              break;
                          case 4:
                              role = '内部员工';
                              break;
                          default :
                              role = '普通用户';
                              break;
                      }

                  } else if (personal_basicInfo_tag.indexOf("birth_date") > -1) {
                      birth_date =  utility.getLocalTime(personal_basicInfo);
                  }
              }
          }

          if (weighted_tags_verrion.indexOf("released_video_tags") > -1) {//发布过的标签
              for (var j = 0; j < weighted_tags_info.length; j++) {
                  var released_video_tag = weighted_tags_info[j].tag;
                  var released_video_tag_count = weighted_tags_info[j].weight;
                  released_video_tags.push(released_video_tag + ":" + released_video_tag_count+"<br/>");//例子：music:71
                  released_video_tags_str+=released_video_tag + ":" + released_video_tag_count+"<br/>";
              }
          }


          if (weighted_tags_verrion.indexOf("liked_video_tags") > -1) {//喜欢的标签
              for (var j = 0; j < weighted_tags_info.length; j++) {
                  var liked_video_tag = weighted_tags_info[j].tag;
                  var liked_video_count = weighted_tags_info[j].weight;
                  liked_video_tags.push(liked_video_tag + ":" + liked_video_count);//例子：music:71
              }
           }
              if (weighted_tags_verrion.indexOf("clicked_video_tags") > -1) {//点击过的标签标签
                  for (var j = 0; j < weighted_tags_info.length; j++) {
                      var clicked_video_tag = weighted_tags_info[j].tag;
                      var clicked_video_count = weighted_tags_info[j].weight;
                      clicked_video_tags.push(clicked_video_tag + ":" + clicked_video_count);//例子：music:71

                  }

              }

         var tr = '<tr><td class="dt-center" colspan="2"><h3>个人信息</h3></td></tr>';
          tr = '<tr><td class="dt-center"><label>性别:</label></td><td class="dt-center">' + gender + '</td></tr>';
          tr += '<tr><td class="dt-center"><label>年龄:</label></td><td class="dt-center">' + age + '</td></tr>';
          tr += '<tr><td class="dt-center"><label>城市:</label></td><td class="dt-center">' + city + '</td></tr>';
          tr += '<tr><td class="dt-center"><label>注册时间:</label></td><td class="dt-center">' + registered_time + '</td></tr>';
          tr += '<tr><td class="dt-center"><label>最近访问时间:</label></td><td class="dt-center">' + last_visit_time + '</td></tr>';
          tr += '<tr><td class="dt-center"><label>个人兴趣标签:</label></td><td class="dt-center">' + personal_interest + '</td></tr>';
          tr += '<tr><td class="dt-center"><label>发布过的标签信息:</label></td><td class="dt-center">' + released_video_tags_str + '</td></tr>';
          tr += '<tr><td class="dt-center"><label>点赞的标签:</label></td><td class="dt-center">' + liked_video_tags.toString() + '</td></tr>';
          tr += '<tr><td class="dt-center"><label>点击过的标签:</label></td><td class="dt-center">' + clicked_video_tags.toString() + '</td></tr>';
          tr += '<tr><td class="dt-center"><label>总发布量:</label></td><td class="dt-center">' + released_video_count + '</td></tr>';
          tr += '<tr><td class="dt-center"><label>被点赞数:</label></td><td class="dt-center">' + accumulated_like_count + '</td></tr>';
          tr += '<tr><td class="dt-center"><label>访客数:</label></td><td class="dt-center">' + accumulated_personalpage_visit_count + '</td></tr>';
          tr += '<tr><td class="dt-center"><label>粉丝数量:</label></td><td class="dt-center">' + accumulated_fans_count + '</td></tr>';
          tr += '<tr><td class="dt-center"><label>手机型号:</label></td><td class="dt-center">' + device_model + '</td></tr>';
          tr += '<tr><td class="dt-center"><label>角色:</label></td><td class="dt-center">' + role + '</td></tr>';
          tr += '<tr><td class="dt-center"><label>出生日期:</label></td><td class="dt-center">' + birth_date + '</td></tr>';

          var html = '<table cellpadding="0" cellspacing="0" class="cell-border result-table"><tbody>' + tr + '</tbody></table>';

          $('.table-container').html(html);

      }
  }
});
