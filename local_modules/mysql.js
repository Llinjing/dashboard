(function moduleMysqlNodejs() {
  /*
    this module exports druid related methods
  */
  'use strict';

  var sf = require('sprintf-js')


  var local = {

    _name: 'request.moduleMysqlNodejs',

    _init: function () {
      EXPORTS.moduleInit(typeof module === 'object' ? module : null, local);
    },


    _getDimKey: function(options, dataSource) {
      var keyTemplate = dataSource.key,
        filterMap = {},
        keyList = keyTemplate.split('|'),
        dimensions,
        keyStr;

      //处理之后每个dimkey从dimensions中对应的name来取对应的值
      dimensions = EXPORTS.getChartDimensionsConfOrigin(options.dataType);
      keyList.forEach(function(k) {
        var kname;
        k = k.replace('%(', '').replace(')s', '');
        if(dimensions[k]&&k!=='interval'){
          kname = dimensions[k].name;
          filterMap[kname] = options.filter && options.filter[kname] && options.filter[kname].values && options.filter[kname].values[0] || '';
          if(k!==kname){
            keyTemplate = keyTemplate.replace(k,kname);
          }
        }else if(k!=='interval'){//dimensions中不包含的该filter则将其设为空添加到filterMap
          filterMap[k] = '';
        }
      });
      if (options.granularity) {
        switch (options.granularity) {
          case 'hour':
            filterMap['interval'] = 'h';
            break;
          case 'day':
          default:
            filterMap['interval'] = 'd';
            break;
        }
      }
      keyStr = sf.sprintf(keyTemplate, filterMap);
      return keyStr;
    },

    _getWhere: function(options, dataSource) {

      var dimensions = EXPORTS.getChartDimensionIdNameMap(options.dataType),
        dimensionsKeys = _.keys(dimensions),
        keyStr = dataSource.key|| [],
        whereArray = [];
      
      dimensionsKeys.forEach(function(d) {
        if (d!=='start_time' && d!=='end_time' && d.substring(0,11)!=='granularity'&& d.substring(0,14)!=='dataSourceType' && d!=='peroid' && keyStr.indexOf(d)<0) {
          var dname = dimensions[d].name, 
              dvalue = options.filter && options.filter[d] && options.filter[d].values && options.filter[d].values[0] || '';
              if(dvalue){
                 if(dname == "impression" || dname == "click"){
                     whereArray.push(dname + '>="' + dvalue + '"');
                     }else{
                     whereArray.push(dname + '="' + dvalue + '"');
                 }
              }
        }


       //时间件处理
          if(d.substring(0,14)=='dataSourceType'){
              var dvalue = JSON.stringify( options['dataSourceType']);

              if(dvalue.indexOf('hourly')>0){
                  dvalue = 1;
              }else if(dvalue.indexOf('daily')>0){
                  dvalue = 2;
              }else if(dvalue.indexOf('10min')>0){
                  dvalue = 3;
              }

              whereArray.push('flag =' + dvalue);
          }
      });

      if (whereArray.length >0)
        return whereArray.join(' AND ') + ' AND';
        //return dataSource.type === 'mysql4' ? whereArray.join(' AND ') : whereArray.join(' AND ')+ ' AND';
      else return '';
    },

    _getMetrics: function(options) {
      var metricsArray = EXPORTS.getChartMetricsConf(options.dataType, 'mysql') || [],
        groupbys = EXPORTS.getChartGroupbyConf(options.dataType) || [],
        chartType = EXPORTS.getChartConfType(options.dataType) || [],
        metricsStrArr = [];
      if (metricsArray.length === 1) {
        return 'Value';
      } else {
        metricsArray.forEach(function(m) {
          var mname = (options.groupBy||(groupbys.length != 0&&chartType != 'table_only')) && 'SUM('+m.name+') AS '+m.name || m.name;
          metricsStrArr.push(mname);
        });
      }
      return metricsStrArr.join(', ');
    },

    
    //change gruopbys
     _getGroupBys: function(options) {

      var idNameMap = EXPORTS.getChartMetricsIdNameMap(options.dataType),
          groupbys = EXPORTS.getChartGroupbyConf(options.dataType) || [],
          chartType = EXPORTS.getChartConfType(options.dataType) || [],
          groupByArray = [];
      if (options.groupBy) {

        options.groupBy.forEach(function(mid) {

          var mname = idNameMap[mid].name;
          groupByArray.push(mname);
          if (_.has(idNameMap[mid],'href')) {
            var hname = idNameMap[idNameMap[mid].href].name;
            groupByArray.push(hname);
          }
        });
        return groupByArray.join(', ');
      }else if(groupbys.length != 0&&chartType != 'table_only'){
        groupbys.forEach(function(mid){
          if(mid.checked == true){
            var mname = mid.name;
            groupByArray.push(mname);
          }
        });
        return groupByArray.join(',');
      }
      else
        return '';
    },

    _getOrderBy: function(options, dataSource) {
      var idNameMap = EXPORTS.getChartMetricsIdNameMap(options.dataType),
        orderbyArray = [];
      if (dataSource.orderby) {
        dataSource.orderby.forEach(function(mid) {
          var mname = idNameMap[mid].name;
          orderbyArray.push(mname);
        });
        return ' ORDER BY '+orderbyArray.join(', ')+' desc ';
      } else return '';
    },

    //llj，mysql语句拼接for druid
   constructSqlTableOnly:function(articleId,dataType){
	var sql = "select content_id, title,link as url from %(table)s where content_id in (%(articleId)s) ",
	//dataSource = "t_content",
	dataSource = "t_content"+"_"+global.moment(new Date()).format('YYYY_MM').toString(),
	sqlTemplate = sql;
	var finalsql = sf.sprintf(sqlTemplate,{
		table:dataSource,
		articleId:articleId.join(',')
	});
	return finalsql;
   },

   //search username
   constructmUsername:function(uids,dataType){
      var sql = "select *  from %(table)s where uid in (%(uids)s) ",
          dataSource = "t_facebook_cms_account_mapping",
          sqlTemplate = sql;
      var finalsql = sf.sprintf(sqlTemplate,{
        table:dataSource,
        uids:uids.join(',')
      });
      return finalsql;
    },
 
   constructIndex:function(options){

      var startTime = global.moment(new Date(options.startTime)).format('YYYY-MM-DD HH:00:00').toString(),
                endTime = global.moment(new Date(options.endTime)).format('YYYY-MM-DD HH:00:00').toString(),
                dataSource = EXPORTS.getChartDataSourceConf(options.dataType)['mysql9'],
                groupbys = EXPORTS.getChartGroupbyConf(options.dataType)|| [],
                chartType = EXPORTS.getChartConfType(options.dataType) || [],
                dateKey = 'timestamp,',
                sqlBase = 'SELECT timestamp,sum(total_request) as total_request,sum(total_request_user) as total_request_user,sum(total_impression) as total_impression,sum(total_impression_user) as total_impression_user,sum(total_click) as total_click,' +
                    'sum(total_click_user) as total_click_user,sum(total_detail_dwell_time) as total_detail_dwell_time,' +
		'sum(total_listpage_dwelltime) as total_listpage_dwelltime,sum(total_listpage_dwelltime_user) as total_listpage_dwelltime_user' +
                    ' FROM %(table)s WHERE %(where)s timestamp BETWEEN "%(start_time)s" AND  "%(end_time)s" ',
                sqlTemplate = ((options.groupBy|| (groupbys.length != 0&&chartType != 'table_only')) && sqlGroupBy || sqlBase) + ' group by timestamp  order by timestamp asc',
                sql;

            if (options.granularity==='all') dateKey = '';

            sql = sf.sprintf(sqlTemplate, {
                date: dateKey,
                metrics: local._getMetrics(options),
                where: local._getWhere(options, dataSource),
                groupby:local._getGroupBys(options),
                orderby:local._getOrderBy(options, dataSource),
                table: dataSource['source']+"_"+options.filter.product_id.values+"_"+options.filter.language.values,
                start_time: startTime,
                end_time: endTime
            });
        return sql;
    },

    constructUserCountry:function(options){
      var startTime = global.moment(new Date(options.startTime)).format('YYYY-MM-DD HH:00:00').toString(),
          endTime = global.moment(new Date(options.endTime)).format('YYYY-MM-DD HH:00:00').toString(),
          dataSource = EXPORTS.getChartDataSourceConf(options.dataType)['mysql_country'],
          groupbys = EXPORTS.getChartGroupbyConf(options.dataType)|| [],
          chartType = EXPORTS.getChartConfType(options.dataType) || [],
          dateKey = 'timestamp,',
          sqlBase = 'SELECT sum(total_impression_user) as total_impression_user FROM %(table)s WHERE %(where)s timestamp>="%(start_time)s" AND  timestamp<"%(end_time)s" ',
          sqlGroupBy = 'SELECT %(date)s %(groupby)s,sum(total_impression_user) as total_impression_user FROM %(table)s WHERE %(where)s timestamp>="%(start_time)s" AND timestamp<"%(end_time)s" GROUP BY %(date)s  %(groupby)s',
          sqlTemplate = ((options.groupBy|| (groupbys.length != 0&&chartType != 'table_only')) && sqlGroupBy || sqlBase) + ' order by timestamp asc',
          sql;

      if (options.granularity==='all') dateKey = '';

      sql = sf.sprintf(sqlTemplate, {
        date: dateKey,
        metrics: local._getMetrics(options),
        where: local._getWhere(options, dataSource),
        groupby:local._getGroupBys(options),
        orderby:local._getOrderBy(options, dataSource),
        table: dataSource['source'],
        start_time: startTime,
        end_time: endTime
      });
      return sql;
    },

    constructPush12hours:function(options){
      var startTime = global.moment(new Date(options.startTime)).format('YYYY-MM-DD HH:00:00').toString(),
          endTime = global.moment(new Date(options.endTime)).format('YYYY-MM-DD HH:00:00').toString(),
          dataSource = EXPORTS.getChartDataSourceConf(options.dataType)['mysql_push12hours'],
          groupbys = EXPORTS.getChartGroupbyConf(options.dataType)|| [],
          chartType = EXPORTS.getChartConfType(options.dataType) || [],
          dateKey = options.granularity,
          sqlBase = 'SELECT %(date)s as timestamp,sum(imp_user) as imp_user,sum(impression) as impression,sum(click_user) as click_user,sum(click) as click FROM %(table)s WHERE %(where)s %(date)s>="%(start_time)s" AND %(date)s<"%(end_time)s" ',
          sqlTemplate = ((options.groupBy|| (groupbys.length != 0&&chartType != 'table_only')) && sqlGroupBy || sqlBase) + ' group by %(date)s order by %(date)s desc',
          sql;

      if (options.granularity==='all') dateKey = '';

      sql = sf.sprintf(sqlTemplate, {
        date: dateKey,
        metrics: local._getMetrics(options),
        where: local._getWhere(options, dataSource),
        groupby:local._getGroupBys(options),
        table: dataSource['source'],
        start_time: startTime,
        end_time: endTime
      });
      return sql;
    },

    constructArticleSharenumSql:function(options){
      var startTime = global.moment(new Date(options.startTime)).format('YYYY-MM-DD HH:mm:00').toString(),
          endTime = global.moment(new Date(options.endTime)).format('YYYY-MM-DD HH:mm:00').toString(),
          dataSource = EXPORTS.getChartDataSourceConf(options.dataType)['mysql_articesharenum'],
          groupbys = EXPORTS.getChartGroupbyConf(options.dataType)|| [],
          chartType = EXPORTS.getChartConfType(options.dataType) || [],
          dateKey = options.granularity,
          sqlBase = 'SELECT sum(robot_shared_count) as robot_shared_count,sum(manually_shared_count) as manually_shared_count,sum(total_shared_count) as total_shared_count FROM %(table)s WHERE %(where)s %(date)s >="%(start_time)s" AND %(date)s<"%(end_time)s"',
          sqlGroupBy = 'SELECT %(date)s as timestamp,%(groupby)s,sum(robot_shared_count) as robot_shared_count,sum(manually_shared_count) as manually_shared_count,sum(total_shared_count) as total_shared_count FROM %(table)s WHERE %(where)s %(date)s >="%(start_time)s" AND  %(date)s<"%(end_time)s" GROUP BY %(date)s,%(groupby)s',
          sqlTemplate = ((options.groupBy|| (groupbys.length != 0&&chartType != 'table_only')) && sqlGroupBy || sqlBase) + '%(orderby)s',
          sql;

      if (options.granularity==='all') dateKey = '';

      sql = sf.sprintf(sqlTemplate, {
        date: dateKey,
        metrics: local._getMetrics(options),
        where: local._getWhere(options, dataSource),
        groupby:local._getGroupBys(options),
        orderby:local._getOrderBy(options, dataSource),
        table: dataSource['source'],
        start_time: startTime,
        end_time: endTime
      });
      return sql;
    },

    constructSharetogroupSql:function(options){
      var startTime = global.moment(new Date(options.startTime)).format('YYYY-MM-DD HH:mm:00').toString(),
          endTime = global.moment(new Date(options.endTime)).format('YYYY-MM-DD HH:mm:00').toString(),
          dataSource = EXPORTS.getChartDataSourceConf(options.dataType)['mysql_groupsharenum'],
          groupbys = EXPORTS.getChartGroupbyConf(options.dataType)|| [],
          chartType = EXPORTS.getChartConfType(options.dataType) || [],
          dateKey = options.granularity,
          sqlBase = 'SELECT sum(shared_num) as shared_num FROM %(table)s WHERE %(where)s %(date)s >="%(start_time)s" AND %(date)s<"%(end_time)s"',
          sqlGroupBy = 'SELECT %(date)s as timestamp,%(groupby)s,sum(shared_num) as shared_num FROM %(table)s WHERE %(where)s %(date)s >="%(start_time)s" AND  %(date)s<"%(end_time)s" GROUP BY %(date)s,%(groupby)s',
          sqlTemplate = ((options.groupBy|| (groupbys.length != 0&&chartType != 'table_only')) && sqlGroupBy || sqlBase) + '%(orderby)s',
          sql;

      if (options.granularity==='all') dateKey = '';

      sql = sf.sprintf(sqlTemplate, {
        date: dateKey,
        metrics: local._getMetrics(options),
        where: local._getWhere(options, dataSource),
        groupby:local._getGroupBys(options),
        orderby:local._getOrderBy(options, dataSource),
        table: dataSource['source'],
        start_time: startTime,
        end_time: endTime
      });
      return sql;
    },

    constructSharearticlenum:function(options){
      var startTime = global.moment(new Date(options.startTime)).format('YYYY-MM-DD HH:00:00').toString(),
          endTime = global.moment(new Date(options.endTime)).format('YYYY-MM-DD HH:00:00').toString(),
          dataSource = EXPORTS.getChartDataSourceConf(options.dataType)['mysql_sharenum'],
          groupbys = EXPORTS.getChartGroupbyConf(options.dataType)|| [],
          chartType = EXPORTS.getChartConfType(options.dataType) || [],
          dateKey = options.granularity,
          sqlBase = 'SELECT %(date)s as timestamp,sum(article_num) as article_num FROM %(table)s WHERE %(where)s %(date)s>="%(start_time)s" AND %(date)s<"%(end_time)s" ',
          sqlTemplate = ((options.groupBy|| (groupbys.length != 0&&chartType != 'table_only')) && sqlGroupBy || sqlBase) + ' group by %(date)s order by %(date)s desc',
          sql;

      if (options.granularity==='all') dateKey = '';

      sql = sf.sprintf(sqlTemplate, {
        date: dateKey,
        metrics: local._getMetrics(options),
        where: local._getWhere(options, dataSource),
        groupby:local._getGroupBys(options),
        table: dataSource['source'],
        start_time: startTime,
        end_time: endTime
      });
      return sql;
    },

   constructUnconsumedarticlenum:function(options){
      var startTime = global.moment(new Date(options.startTime)).format('YYYY-MM-DD HH:00:00').toString(),
          endTime = global.moment(new Date(options.endTime)).format('YYYY-MM-DD HH:00:00').toString(),
          dataSource = EXPORTS.getChartDataSourceConf(options.dataType)['mysql_unconsumed'],
          groupbys = EXPORTS.getChartGroupbyConf(options.dataType)|| [],
          chartType = EXPORTS.getChartConfType(options.dataType) || [],
          dateKey = options.granularity,
          sqlBase = 'SELECT %(date)s as timestamp,sum(unclaimed_num) as unclaimed_num FROM %(table)s WHERE %(where)s %(date)s>="%(start_time)s" AND %(date)s<"%(end_time)s" ',
          sqlTemplate = ((options.groupBy|| (groupbys.length != 0&&chartType != 'table_only')) && sqlGroupBy || sqlBase) + ' group by %(date)s order by %(date)s desc',
          sql;

      if (options.granularity==='all') dateKey = '';

      sql = sf.sprintf(sqlTemplate, {
        date: dateKey,
        metrics: local._getMetrics(options),
        where: local._getWhere(options, dataSource),
        groupby:local._getGroupBys(options),
        table: dataSource['source'],
        start_time: startTime,
        end_time: endTime
      });
      return sql;
    },
    
    constructH5SharePVSql:function(options){
      var startTime = global.moment(new Date(options.startTime)).format('YYYY-MM-DD HH:00:00').toString(),
          endTime = global.moment(new Date(options.endTime)).format('YYYY-MM-DD HH:00:00').toString(),
          dataSource = EXPORTS.getChartDataSourceConf(options.dataType)['mysql_h5pv'],
          groupbys = EXPORTS.getChartGroupbyConf(options.dataType)|| [],
          chartType = EXPORTS.getChartConfType(options.dataType) || [],
          dateKey = options.granularity,          
          sqlBase = 'SELECT sum(h5_url_click) as h5_url_click,sum(page_view) as page_view,sum(click_download_app) as click_download_app,sum(install_num) as install_num FROM %(table)s WHERE %(where)s %(date)s>="%(start_time)s" AND %(date)s<"%(end_time)s"',          
          sqlGroupBy = 'SELECT %(date)s as timestamp, %(groupby)s,sum(h5_url_click) as h5_url_click,sum(page_view) as page_view,sum(click_download_app) as click_download_app,sum(install_num) as install_num FROM %(table)s WHERE %(where)s %(date)s>="%(start_time)s" AND %(date)s<"%(end_time)s" GROUP BY %(date)s,%(groupby)s',
          sqlTemplate = ((options.groupBy|| (groupbys.length != 0&&chartType != 'table_only')) && sqlGroupBy || sqlBase) + ' order by h5_url_click desc',
          sql;
      
      if (options.granularity==='all') dateKey = '';
      
      sql = sf.sprintf(sqlTemplate, {
        date: dateKey,
        metrics: local._getMetrics(options),
        where: local._getWhere(options, dataSource),
        groupby:local._getGroupBys(options),
        orderby:local._getOrderBy(options, dataSource),
        table: dataSource['source'],
        start_time: startTime,
        end_time: endTime
      });
      return sql;
    },

    constructH5ShareUVSql:function(options){
      var startTime = global.moment(new Date(options.startTime)).format('YYYY-MM-DD HH:00:00').toString(),
          endTime = global.moment(new Date(options.endTime)).format('YYYY-MM-DD HH:00:00').toString(),
          dataSource = EXPORTS.getChartDataSourceConf(options.dataType)['mysql_h5uv'],
          groupbys = EXPORTS.getChartGroupbyConf(options.dataType)|| [],
          chartType = EXPORTS.getChartConfType(options.dataType) || [],
          dateKey = 'timestamp,',
          sqlBase = 'SELECT sum(h5_url_click_user) as h5_url_click_user,sum(page_view_user) as page_view_user,sum(download_app_user) as download_app_user,sum(install_app_user) as install_app_user FROM %(table)s WHERE %(where)s timestamp >="%(start_time)s" AND timestamp<"%(end_time)s"',
          sqlGroupBy = 'SELECT %(date)s %(groupby)s,sum(h5_url_click_user) as h5_url_click_user,sum(page_view_user) as page_view_user,sum(download_app_user) as download_app_user,sum(install_app_user) as install_app_user FROM %(table)s WHERE %(where)s timestamp >="%(start_time)s" AND  timestamp<"%(end_time)s" GROUP BY %(date)s %(groupby)s',
          sqlTemplate = ((options.groupBy|| (groupbys.length != 0&&chartType != 'table_only')) && sqlGroupBy || sqlBase) + '%(orderby)s',
          sql;

      if (options.granularity==='all') dateKey = '';

      sql = sf.sprintf(sqlTemplate, {
        date: dateKey,
        metrics: local._getMetrics(options),
        where: local._getWhere(options, dataSource),
        groupby:local._getGroupBys(options),
        orderby:local._getOrderBy(options, dataSource),
        table: dataSource['source'],
        start_time: startTime,
        end_time: endTime
      });
      return sql;
    },

    constructH5ShareSql:function(options){
      var startTime = global.moment(new Date(options.startTime)).format('YYYY-MM-DD HH:mm:00').toString(),
          endTime = global.moment(new Date(options.endTime)).format('YYYY-MM-DD HH:mm:00').toString(),
          dataSource = EXPORTS.getChartDataSourceConf(options.dataType)['mysql_h5share'],
          groupbys = EXPORTS.getChartGroupbyConf(options.dataType)|| [],
          chartType = EXPORTS.getChartConfType(options.dataType) || [],
          dateKey = 'timestamp,',
          sqlBase = 'SELECT sum(num) as num FROM %(table)s WHERE %(where)s timestamp >="%(start_time)s" AND timestamp<"%(end_time)s"',
          sqlGroupBy = 'SELECT %(date)s %(groupby)s,sum(num) as num FROM %(table)s WHERE %(where)s timestamp >="%(start_time)s" AND  timestamp<"%(end_time)s" GROUP BY %(date)s %(groupby)s',
          sqlTemplate = ((options.groupBy|| (groupbys.length != 0&&chartType != 'table_only')) && sqlGroupBy || sqlBase) + '%(orderby)s',
          sql;

      if (options.granularity==='all') dateKey = '';

      sql = sf.sprintf(sqlTemplate, {
        date: dateKey,
        metrics: local._getMetrics(options),
        where: local._getWhere(options, dataSource),
        groupby:local._getGroupBys(options),
        orderby:local._getOrderBy(options, dataSource),
        table: dataSource['source'],
        start_time: startTime,
        end_time: endTime
      });
      return sql;
    },

    constructHistoricalSql:function(options){
      var startTime = global.moment(new Date(options.startTime)).format('YYYY-MM-DD HH:00:00').toString(),
          endTime = global.moment(new Date(options.endTime)).format('YYYY-MM-DD HH:00:00').toString(),
          dataSource = EXPORTS.getChartDataSourceConf(options.dataType)['mysql_histority'],
          groupbys = EXPORTS.getChartGroupbyConf(options.dataType)|| [],
          chartType = EXPORTS.getChartConfType(options.dataType) || [],
          dateKey = 'submit_time ',
          sqlBase = 'SELECT %(metrics)s FROM %(table)s WHERE %(where)s submit_time >="%(start_time)s" AND  submit_time<"%(end_time)s"',
          sqlGroupBy = 'SELECT %(groupby)s, %(metrics)s FROM %(table)s WHERE %(where)s submit_time >="%(start_time)s" AND  submit_time<"%(end_time)s"  GROUP BY %(groupby)s',
          sqlTemplate = ((options.groupBy|| (groupbys.length != 0&&chartType != 'table_only')) && sqlGroupBy || sqlBase) + '%(orderby)s',
          sql;

      if (options.granularity==='all') dateKey = '';

      sql = sf.sprintf(sqlTemplate, {
        date: dateKey,
        metrics: local._getMetrics(options),
        where: local._getWhere(options, dataSource),
        groupby:local._getGroupBys(options),
        orderby:local._getOrderBy(options, dataSource),
        table: dataSource['source'],
        start_time: startTime,
        end_time: endTime
      });
      return sql;
    },

    constructExplore:function(options){
      var startTime = global.moment(new Date(options.startTime)).format('YYYY-MM-DD HH:00:00').toString(),
          endTime = global.moment(new Date(options.endTime)).format('YYYY-MM-DD HH:00:00').toString(),
          dataSource = EXPORTS.getChartDataSourceConf(options.dataType)['mysql_explore'],
          groupbys = EXPORTS.getChartGroupbyConf(options.dataType)|| [],
          chartType = EXPORTS.getChartConfType(options.dataType) || [],
          dateKey = 'timestamp,',
          sqlBase = 'SELECT timestamp, sum(explore_pool) as explore_pool,sum(exploring_pool) as exploring_pool,sum(explore_eagerly_pool) as explore_eagerly_pool  FROM %(table)s WHERE %(where)s timestamp>="%(start_time)s" AND timestamp<"%(end_time)s" ',
          sqlTemplate = ((options.groupBy|| (groupbys.length != 0&&chartType != 'table_only')) && sqlGroupBy || sqlBase) + ' group by timestamp order by timestamp asc',
          sql;

      if (options.granularity==='all') dateKey = '';

      sql = sf.sprintf(sqlTemplate, {
        date: dateKey,
        metrics: local._getMetrics(options),
        where: local._getWhere(options, dataSource),
        groupby:local._getGroupBys(options),
        orderby:local._getOrderBy(options, dataSource),
        table: dataSource['source'],
        start_time: startTime,
        end_time: endTime
      });
      return sql;
    },

    constructAdDaily: function(options){
            var startTime = global.moment(new Date(options.startTime)).format('YYYY-MM-DD HH:00:00').toString(),
                endTime = global.moment(new Date(options.endTime)).format('YYYY-MM-DD HH:00:00').toString(),
                dataSource = EXPORTS.getChartDataSourceConf(options.dataType)['mysql_ad_daily'],
                groupbys = EXPORTS.getChartGroupbyConf(options.dataType)|| [],
                chartType = EXPORTS.getChartConfType(options.dataType) || [],
                dateKey = options.granularity,
                sqlBase = 'SELECT %(date)s as timestamp,sum(ad_request) as ad_request,sum(ad_request_fill) as ad_request_fill,sum(adspace_impression) as adspace_impression,sum(ad_demand_fill_timeout) as ad_demand_fill_timeout,sum(ad_demand_nofill) as ad_demand_nofill,sum(ad_demand) as ad_demand, sum(request_call) as request_call,sum(request) as request,sum(ad_demand_fill) as ad_demand_fill,sum(impression) as impression,sum(click) as click FROM %(table)s WHERE %(where)s %(date)s>="%(start_time)s" AND %(date)s<"%(end_time)s" ',
                sqlGroupBy = options.granularity=="all"?'SELECT %(groupby)s,sum(ad_request) as ad_request,sum(ad_request_fill) as ad_request_fill,sum(adspace_impression) as adspace_impression,sum(ad_demand_fill_timeout) as ad_demand_fill_timeout,sum(ad_demand_nofill) as ad_demand_nofill,sum(ad_demand) as ad_demand, sum(request_call) as request_call,sum(request) as request,sum(ad_demand_fill) as ad_demand_fill,sum(impression) as impression,sum(click) as click FROM %(table)s WHERE %(where)s timestamp_hour>="%(start_time)s" AND timestamp_hour<"%(end_time)s" GROUP BY %(groupby)s':'SELECT %(date)s as timestamp, %(groupby)s,sum(ad_request) as ad_request,sum(ad_request_fill) as ad_request_fill,sum(adspace_impression) as adspace_impression,sum(ad_demand_fill_timeout) as ad_demand_fill_timeout,sum(ad_demand_nofill) as ad_demand_nofill,sum(ad_demand) as ad_demand, sum(request_call) as request_call,sum(request) as request,sum(ad_demand_fill) as ad_demand_fill,sum(impression) as impression,sum(click) as click FROM %(table)s WHERE %(where)s %(date)s>="%(start_time)s" AND %(date)s<"%(end_time)s" GROUP BY %(date)s ,%(groupby)s',
                sqlTemplate = ((options.groupBy|| (groupbys.length != 0&&chartType != 'table_only')) && sqlGroupBy || sqlBase) + '%(orderby)s limit 1000',
                sql;

            if (options.granularity==='all') dateKey = '';

            sql = sf.sprintf(sqlTemplate, {
                date: dateKey,
                metrics: local._getMetrics(options),
                where: local._getWhere(options, dataSource),
                groupby:local._getGroupBys(options),
                orderby:local._getOrderBy(options, dataSource),
                table: dataSource['source'],
                start_time: startTime,
                end_time: endTime
            });
            return sql;
    },

   constructADNewsIndicator:function(options){
      var startTime = global.moment(new Date(options.startTime)).format('YYYY-MM-DD HH:00:00').toString(),
          endTime = global.moment(new Date(options.endTime)).format('YYYY-MM-DD HH:00:00').toString(),
          dataSource = EXPORTS.getChartDataSourceConf(options.dataType)['mysql_ad_news'],
          groupbys = EXPORTS.getChartGroupbyConf(options.dataType)|| [],
          chartType = EXPORTS.getChartConfType(options.dataType) || [],
          dateKey = 'date,',
          sqlBase = 'SELECT date as timestamp,sum(long_listpage_impression) as long_listpage_impression,sum(long_listpage_click) as long_listpage_click,sum(push_click) as push_click,sum(quickread_click) as quickread_click,sum(lockscreen_impression) as lockscreen_impression,sum(lockscreen_click) as lockscreen_click,sum(relevant_recommendation_impression) as relevant_recommendation_impression,sum(relevant_recommendation_click) as relevant_recommendation_click FROM %(table)s WHERE %(where)s date>="%(start_time)s" AND date<"%(end_time)s" group by date',
          sqlGroupBy = 'SELECT date as timestamp,%(groupby)s,sum(long_listpage_impression) as long_listpage_impression,sum(long_listpage_click) as long_listpage_click,sum(push_click) as push_click,sum(quickread_click) as quickread_click,sum(lockscreen_impression) as lockscreen_impression,sum(lockscreen_click) as lockscreen_click,sum(relevant_recommendation_impression) as relevant_recommendation_impression,sum(relevant_recommendation_click) as relevant_recommendation_click FROM %(table)s WHERE %(where)s date>="%(start_time)s" AND  date<"%(end_time)s" GROUP BY %(date)s %(groupby)s',
          sqlTemplate = ((options.groupBy|| (groupbys.length != 0&&chartType != 'table_only')) && sqlGroupBy || sqlBase) + ' order by date desc',
          sql;

      if (options.granularity==='all') dateKey = '';

      sql = sf.sprintf(sqlTemplate, {
        date: dateKey,
        metrics: local._getMetrics(options),
        where: local._getWhere(options, dataSource),
        groupby:local._getGroupBys(options),
        orderby:local._getOrderBy(options, dataSource),
        table: dataSource['source'],
        start_time: startTime,
        end_time: endTime
      });
      return sql;
    }, 

    constructPushServerNew:function(options){
      var startTime = global.moment(new Date(options.startTime)).format('YYYY-MM-DD HH:00:00').toString(),
          endTime = global.moment(new Date(options.endTime)).format('YYYY-MM-DD HH:00:00').toString(),
          dataSource = EXPORTS.getChartDataSourceConf(options.dataType)['mysql_pushserver_new'],
          groupbys = EXPORTS.getChartGroupbyConf(options.dataType)|| [],
          chartType = EXPORTS.getChartConfType(options.dataType) || [],
          dateKey = 'time,',
          sqlBase = 'SELECT time as timestamp,id,label,promotion_appver_tag,push_batch_index,content_id,product,autoinc_push_news_id,strategy,total,dispatch_num,success_num,failed_num,push_impression_30mins,push_click_30mins,push_impression_2hours,push_click_2hours,push_impression_24hours,push_click_24hours FROM %(table)s WHERE %(where)s time>="%(start_time)s" AND time<"%(end_time)s" ',
          sqlGroupBy = 'SELECT time as timestamp,%(groupby)s,id,promotion_appver_tag,content_id,push_batch_index,product,autoinc_push_news_id,strategy,sum(total) as total,sum(dispatch_num) as dispatch_num,sum(success_num) as success_num,sum(failed_num) as failed_num,sum(push_impression_30mins) as push_impression_30mins,sum(push_click_30mins) as push_click_30mins,sum(push_impression_2hours) as push_impression_2hours,sum(push_click_2hours) as push_click_2hours,sum(push_impression_24hours) as push_impression_24hours,sum(push_click_24hours) as push_click_24hours FROM %(table)s WHERE %(where)s time>="%(start_time)s" AND  time<"%(end_time)s" GROUP BY %(date)s strategy, %(groupby)s',
          sqlTemplate = ((options.groupBy|| (groupbys.length != 0&&chartType != 'table_only')) && sqlGroupBy || sqlBase) + ' order by id desc',
          sql;

      if (options.granularity==='all') dateKey = '';

      sql = sf.sprintf(sqlTemplate, {
        date: dateKey,
        metrics: local._getMetrics(options),
        where: local._getWhere(options, dataSource),
        groupby:local._getGroupBys(options),
        orderby:local._getOrderBy(options, dataSource),
        table: dataSource['source'],
        start_time: startTime,
        end_time: endTime
      });
      return sql;
    },

    construcADReportServer:function(options){
      var startTime = global.moment(new Date(options.startTime)).format('YYYY-MM-DD HH:00:00').toString(),
          endTime = global.moment(new Date(options.endTime)).format('YYYY-MM-DD HH:00:00').toString(),
          dataSource = EXPORTS.getChartDataSourceConf(options.dataType)['mysql_ad_report'],
          groupbys = EXPORTS.getChartGroupbyConf(options.dataType)|| [],
          chartType = EXPORTS.getChartConfType(options.dataType) || [],
          dateKey = 'date',
          sqlBase = 'SELECT date as timestamp,sum(request) as request,sum(requestFill) as requestFill,sum(impression) as impression,sum(click) as click,sum(revenue) as revenue FROM %(table)s WHERE %(where)s date>="%(start_time)s" AND date<"%(end_time)s" group by date ',
          sqlGroupBy = 'SELECT date as timestamp,%(groupby)s,sum(request) as request,sum(requestFill) as requestFill,sum(impression) as impression,sum(click) as click,sum(revenue) as revenue FROM %(table)s WHERE %(where)s date>="%(start_time)s" AND  date<"%(end_time)s" GROUP BY %(date)s,%(groupby)s',
          sqlTemplate = ((options.groupBy|| (groupbys.length != 0&&chartType != 'table_only')) && sqlGroupBy || sqlBase) + ' order by date desc',
          sql;

      if (options.granularity==='all') dateKey = '';

      sql = sf.sprintf(sqlTemplate, {
        date: dateKey,
        metrics: local._getMetrics(options),
        where: local._getWhere(options, dataSource),
        groupby:local._getGroupBys(options),
        orderby:local._getOrderBy(options, dataSource),
        table: dataSource['source'],
        start_time: startTime,
        end_time: endTime
      });
      return sql;
    },

    constructADDailyChart:function(options){
      var startTime = global.moment(new Date(options.startTime)).format('YYYY-MM-DD HH:00:00').toString(),
          endTime = global.moment(new Date(options.endTime)).format('YYYY-MM-DD HH:00:00').toString(),
          dataSource = EXPORTS.getChartDataSourceConf(options.dataType)['mysql_ad_chart'],
          groupbys = EXPORTS.getChartGroupbyConf(options.dataType)|| [],
          chartType = EXPORTS.getChartConfType(options.dataType) || [],
          dateKey = options.granularity,
          sqlBase = 'SELECT %(date)s as timestamp,sum(ad_request) as ad_request,sum(ad_request_fill) as ad_request_fill,sum(adspace_impression) as adspace_impression,sum(ad_demand_fill_timeout) as ad_demand_fill_timeout,sum(ad_demand_nofill) as ad_demand_nofill,sum(ad_demand) as ad_demand, sum(request_call) as request_call,sum(request) as request,sum(ad_demand_fill) as ad_demand_fill,sum(impression) as impression,sum(click) as click FROM %(table)s WHERE %(where)s %(date)s>="%(start_time)s" AND %(date)s<"%(end_time)s" ',
          sqlTemplate = ((options.groupBy|| (groupbys.length != 0&&chartType != 'table_only')) && sqlGroupBy || sqlBase) + ' group by %(date)s order by %(date)s asc',
          sql;

      if (options.granularity==='all') dateKey = '';

      sql = sf.sprintf(sqlTemplate, {
        date: dateKey,
        metrics: local._getMetrics(options),
        where: local._getWhere(options, dataSource),
        groupby:local._getGroupBys(options),
        orderby:local._getOrderBy(options, dataSource),
        table: dataSource['source'],
        start_time: startTime,
        end_time: endTime
      });
      return sql;
    },

    constructDailyH5Sql:function(options){
      var //startTime = new Date(options.startTime).getTime()-state.env['country2utc'][options.filter["product_id"]["values"]],
          //startTime = global.moment(startTime).format('YYYY-MM-DD 00:00:ss').toString(),
          //endTime = new Date(options.endTime).getTime()-state.env['country2utc'][options.filter["product_id"]["values"]],
          //endTime = global.moment(endTime).format('YYYY-MM-DD 00:00:ss').toString(),
          startTime = global.moment(new Date(options.startTime)).format('YYYY-MM-DD HH:00:00').toString(),
          endTime = global.moment(new Date(options.endTime)).format('YYYY-MM-DD HH:00:00').toString(),
          dataSource = EXPORTS.getChartDataSourceConf(options.dataType)['mysql_h5'],
          groupbys = EXPORTS.getChartGroupbyConf(options.dataType)|| [],
          chartType = EXPORTS.getChartConfType(options.dataType) || [],
          //dateKey = 'timestamp,',
          dateKey =  options.granularity,
          sqlBase = 'SELECT sum(page_view_request) as page_view_request FROM %(table)s WHERE %(where)s %(date)s>="%(start_time)s" AND %(date)s<"%(end_time)s"',
          //sqlGroupBy =options.granularity=="timestamp_hour"||"timestamp_day"?'SELECT %(date)s as timestamp, %(groupby)s,sum(page_view_request) as page_view_request FROM %(table)s WHERE %(where)s %(date)s>="%(start_time)s" AND %(date)s<"%(end_time)s" GROUP BY %(date)s,%(groupby)s':'SELECT timestamp_hour as timestamp, %(groupby)s,sum(page_view_request) as page_view_request FROM %(table)s WHERE %(where)s timestamp_hour>="%(start_time)s" AND timestamp_hour<"%(end_time)s" GROUP BY %(groupby)s',
          //sqlTemplate = ((options.groupBy|| (groupbys.length != 0&&chartType != 'table_only')) && sqlGroupBy || sqlBase) + ' ORDER BY page_view_request desc',
          //sqlBase = 'SELECT sum(page_view_request) as page_view_request FROM %(table)s WHERE %(where)s %(date)s>="%(start_time)s" AND %(date)s<"%(end_time)s"',
          sqlGroupBy = 'SELECT %(date)s as timestamp, %(groupby)s,sum(page_view_request) as page_view_request FROM %(table)s WHERE %(where)s %(date)s>="%(start_time)s" AND %(date)s<"%(end_time)s" GROUP BY %(date)s,%(groupby)s',
          sqlTemplate = ((options.groupBy|| (groupbys.length != 0&&chartType != 'table_only')) && sqlGroupBy || sqlBase) + ' ORDER BY page_view_request desc',
          sql;

      if (options.granularity==='all') dateKey = '';

      sql = sf.sprintf(sqlTemplate, {
        date: dateKey,
        metrics: local._getMetrics(options),
        where: local._getWhere(options, dataSource),
        groupby:local._getGroupBys(options),
        orderby:local._getOrderBy(options, dataSource),
        table: dataSource['source'],
        start_time: startTime,
        end_time: endTime
      });
      return sql;
    },

    constructmNewuserltvSql:function(options){
      var startTime = global.moment(new Date(options.startTime)).format('YYYY-MM-DD HH:00:00').toString(),
          endTime = global.moment(new Date(options.endTime)).format('YYYY-MM-DD HH:00:00').toString(),
          dataSource = EXPORTS.getChartDataSourceConf(options.dataType)['mysql_new_ltv'],
          groupbys = EXPORTS.getChartGroupbyConf(options.dataType)|| [],
          chartType = EXPORTS.getChartConfType(options.dataType) || [],
          dateKey = 'timestamp,',
          sqlBase = 'SELECT timestamp,sum(users_num) as users_num,sum(impression) as impression,sum(click) as click,sum(listpage_dwelltime) as listpage_dwelltime,sum(detail_dwelltime) as detail_dwelltime FROM %(table)s WHERE %(where)s timestamp >="%(start_time)s" AND timestamp<"%(end_time)s"',
          sqlGroupBy = 'SELECT %(date)s %(groupby)s, sum(users_num) as users_num,sum(impression) as impression,sum(click) as click,sum(listpage_dwelltime) as listpage_dwelltime,sum(detail_dwelltime) as detail_dwelltime FROM %(table)s WHERE %(where)s timestamp >="%(start_time)s" AND  timestamp<"%(end_time)s" GROUP BY %(date)s %(groupby)s',
          sqlTemplate = ((options.groupBy|| (groupbys.length != 0&&chartType != 'table_only')) && sqlGroupBy || sqlBase) + '%(orderby)s',
          sql;

      if (options.granularity==='all') dateKey = '';

      sql = sf.sprintf(sqlTemplate, {
        date: dateKey,
        metrics: local._getMetrics(options),
        where: local._getWhere(options, dataSource),
        groupby:local._getGroupBys(options),
        orderby:local._getOrderBy(options, dataSource),
        table: dataSource['source'],
        start_time: startTime,
        end_time: endTime
      });
      return sql;
    },

    constructBallotUserSql:function(options){
      var startTime = global.moment(new Date(options.startTime)).format('YYYY-MM-DD HH:00:00').toString(),
          endTime = global.moment(new Date(options.endTime)).format('YYYY-MM-DD HH:00:00').toString(),
          dataSource = EXPORTS.getChartDataSourceConf(options.dataType)['mysql_ballot_user'],
          groupbys = EXPORTS.getChartGroupbyConf(options.dataType)|| [],
          chartType = EXPORTS.getChartConfType(options.dataType) || [],
          dateKey = 'timestamp ,',
          sqlBase = 'SELECT timestamp,sum(boring_user_num) as boring_user_num,sum(boring_num) as boring_num,sum(like_user_num) as like_user_num,sum(like_num) as like_num,sum(angry_user_num) as angry_user_num,sum(angry_num) as angry_num,sum(sad_user_num) as sad_user_num,sum(sad_num) as sad_num,sum(ballot_user_num) as ballot_user_num,sum(ballot_num) as ballot_num FROM %(table)s WHERE %(where)s timestamp >="%(start_time)s" AND  timestamp<"%(end_time)s" group by timestamp ',
          sqlGroupBy = 'SELECT %(date)s %(groupby)s, %(metrics)s FROM %(table)s WHERE %(where)s timestamp >="%(start_time)s" AND  timestamp<"%(end_time)s" GROUP BY %(date)s %(groupby)s',
          sqlTemplate = ((options.groupBy|| (groupbys.length != 0&&chartType != 'table_only')) && sqlGroupBy || sqlBase) + '%(orderby)s',
          sql;

      if (options.granularity==='all') dateKey = '';

      sql = sf.sprintf(sqlTemplate, {
        date: dateKey,
        metrics: local._getMetrics(options),
        where: local._getWhere(options, dataSource),
        groupby:local._getGroupBys(options),
        orderby:local._getOrderBy(options, dataSource),
        table: dataSource['source'],
        start_time: startTime,
        end_time: endTime
      });
      return sql;
    },

    constructBallotArticleSql:function(options){
      var startTime = global.moment(new Date(options.startTime)).format('YYYY-MM-DD HH:00:00').toString(),
          endTime = global.moment(new Date(options.endTime)).format('YYYY-MM-DD HH:00:00').toString(),
          dataSource = EXPORTS.getChartDataSourceConf(options.dataType)['mysql_ballot_article'],
          groupbys = EXPORTS.getChartGroupbyConf(options.dataType)|| [],
          chartType = EXPORTS.getChartConfType(options.dataType) || [],
          dateKey = 'timestamp ,',
          sqlBase = 'SELECT %(metrics)s FROM %(table)s WHERE %(where)s timestamp >="%(start_time)s" AND  timestamp<"%(end_time)s"',
          sqlGroupBy = 'SELECT %(date)s %(groupby)s, sum(like_num) as like_num,sum(boring_num) as boring_num,sum(angry_num) as angry_num,sum(sad_num) as sad_num,sum(click) as click,sum(request) as request,sum(impression) as impression,sum(dwelltime) as dwelltime FROM %(table)s WHERE %(where)s timestamp >="%(start_time)s" AND  timestamp<"%(end_time)s" GROUP BY %(date)s %(groupby)s',
          sqlTemplate = ((options.groupBy|| (groupbys.length != 0&&chartType != 'table_only')) && sqlGroupBy || sqlBase) + ' order by request desc',
          sql;

      if (options.granularity==='all') dateKey = '';

      sql = sf.sprintf(sqlTemplate, {
        date: dateKey,
        metrics: local._getMetrics(options),
        where: local._getWhere(options, dataSource),
        groupby:local._getGroupBys(options),
        orderby:local._getOrderBy(options, dataSource),
        table: dataSource['source'],
        start_time: startTime,
        end_time: endTime
      });
      return sql;
    },

    constructmMediapvSql:function(options){
      var startTime = global.moment(new Date(options.startTime)).format('YYYY-MM-DD HH:00:00').toString(),
          endTime = global.moment(new Date(options.endTime)).format('YYYY-MM-DD HH:00:00').toString(),
          dataSource = EXPORTS.getChartDataSourceConf(options.dataType)['mysql_media'],
          groupbys = EXPORTS.getChartGroupbyConf(options.dataType)|| [],
          chartType = EXPORTS.getChartConfType(options.dataType) || [],
          dateKey = 'timestamp ,',
          sqlBase = 'SELECT timestamp,sum(request_all) as request_all,sum(impression_all) as impression_all,sum(request) as request,sum(request_user) as request_user,sum(impression) as impression,sum(impression_user) as impression_user,sum(click) as click,sum(click_user) as click_user,sum(dwelltime) as dwelltime,sum(available_article_amount) as available_article_amount FROM %(table)s WHERE %(where)s timestamp >="%(start_time)s" AND timestamp<"%(end_time)s"',
          sqlGroupBy = 'SELECT %(date)s %(groupby)s, sum(request_all) as request_all,sum(impression_all) as impression_all,sum(request) as request,sum(request_user) as request_user,sum(impression) as impression,sum(impression_user) as impression_user,sum(click) as click,sum(click_user) as click_user,sum(dwelltime) as dwelltime,sum(available_article_amount) as available_article_amount FROM %(table)s WHERE %(where)s timestamp >="%(start_time)s" AND  timestamp<"%(end_time)s" GROUP BY %(date)s %(groupby)s',
          sqlTemplate = ((options.groupBy|| (groupbys.length != 0&&chartType != 'table_only')) && sqlGroupBy || sqlBase) + '%(orderby)s',
          sql;

      if (options.granularity==='all') dateKey = '';

      sql = sf.sprintf(sqlTemplate, {
        date: dateKey,
        metrics: local._getMetrics(options),
        where: local._getWhere(options, dataSource),
        groupby:local._getGroupBys(options),
        orderby:local._getOrderBy(options, dataSource),
        table: dataSource['source'],
        start_time: startTime,
        end_time: endTime
      });
      return sql;
    },

    constructAppsflyerSql:function(options){
      var startTime = global.moment(new Date(options.startTime)).format('YYYY-MM-DD HH:00:00').toString(),
          endTime = global.moment(new Date(options.endTime)).format('YYYY-MM-DD HH:00:00').toString(),
          dataSource = EXPORTS.getChartDataSourceConf(options.dataType)['mysql_appsflyer'],
          groupbys = EXPORTS.getChartGroupbyConf(options.dataType)|| [],
          chartType = EXPORTS.getChartConfType(options.dataType) || [],
          dateKey = 'timestamp,',
          sqlBase = 'SELECT %(date)s sum(new_user) as new_user,sum(appsflyer_user) as appsflyer_user,sum(application_user) as application_user,sum(activity_user) as activity_user,sum(impression_user) as impression_user,sum(click_user) as click_user  FROM %(table)s WHERE %(where)s timestamp>="%(start_time)s" AND timestamp<"%(end_time)s" group by timestamp',
          sqlGroupBy = 'SELECT %(date)s  %(groupby)s, sum(new_user) as new_user,sum(appsflyer_user) as appsflyer_user,sum(application_user) as application_user,sum(activity_user) as activity_user,sum(impression_user) as impression_user,sum(click_user) as click_user FROM %(table)s WHERE %(where)s timestamp>="%(start_time)s" AND timestamp<"%(end_time)s" GROUP BY %(date)s %(groupby)s',
          sqlTemplate = ((options.groupBy|| (groupbys.length != 0&&chartType != 'table_only')) && sqlGroupBy || sqlBase) + ' order by timestamp desc ',
          sql;

      if (options.granularity==='all') dateKey = '';

      sql = sf.sprintf(sqlTemplate, {
        date: dateKey,
        metrics: local._getMetrics(options),
        where: local._getWhere(options, dataSource),
        groupby:local._getGroupBys(options),
        orderby:local._getOrderBy(options, dataSource),
        table: dataSource['source'],
        start_time: startTime,
        end_time: endTime
      });
      return sql;
    },
 
    constructClickpushsql:function(options){
      var startTime,
          endTime;
      startTime = new Date(options.startTime).getTime()-state.env['country2utc'][options.filter["product_id"]["values"]];
      startTime = global.moment(startTime).format('YYYY-MM-DD HH:mm:ss').toString();
      endTime = new Date(options.endTime).getTime()-state.env['country2utc'][options.filter["product_id"]["values"]];
      endTime = global.moment(endTime).format('YYYY-MM-DD HH:mm:ss').toString();
      var //startTime = global.moment(new Date(options.startTime)).format('YYYY-MM-DD HH:00:00').toString(),
          //endTime = global.moment(new Date(options.endTime)).format('YYYY-MM-DD HH:00:00').toString(),
          dataSource = EXPORTS.getChartDataSourceConf(options.dataType)['mysql_clickpush'],
          groupbys = EXPORTS.getChartGroupbyConf(options.dataType)|| [],
          chartType = EXPORTS.getChartConfType(options.dataType) || [],
          dateKey = 'timestamp,',
          sqlBase = 'SELECT push_time,sum(back2list_click) as back2list_click,sum(impression_push_user) as impression_push_user,sum(click_push_user) as click_push_user,sum(back2list_impression_user) as impression_user,sum(back2list_impression) as impression FROM %(table)s WHERE %(where)s push_time BETWEEN "%(start_time)s" AND  "%(end_time)s"',
          sqlGroupBy = 'SELECT %(date)s %(groupby)s,push_time,sum(back2list_click) as back2list_click,sum(impression_push_user) as impression_push_user,sum(click_push_user) as click_push_user,sum(back2list_impression_user) as impression_user,sum(back2list_impression) as impression FROM %(table)s WHERE %(where)s push_time>="%(start_time)s" AND push_time<"%(end_time)s"  GROUP BY %(groupby)s',
          sqlTemplate = ((options.groupBy|| (groupbys.length != 0&&chartType != 'table_only')) && sqlGroupBy || sqlBase) + '%(orderby)s',
          sql;

      if (options.granularity==='all') dateKey = '';

      sql = sf.sprintf(sqlTemplate, {
        date: dateKey,
        metrics: local._getMetrics(options),
        where: local._getWhere(options, dataSource),
        groupby:local._getGroupBys(options),
        orderby:local._getOrderBy(options, dataSource),
        table: dataSource['source'],
        start_time: startTime,
        end_time: endTime
      });
      return sql;
    },
 
    constructPushclickSql:function(options){
      var startTime = global.moment(new Date(options.startTime)).format('YYYY-MM-DD HH:00:00').toString(),
          endTime = global.moment(new Date(options.endTime)).format('YYYY-MM-DD HH:00:00').toString(),
          dataSource = EXPORTS.getChartDataSourceConf(options.dataType)['mysql_pushclick'],
          groupbys = EXPORTS.getChartGroupbyConf(options.dataType)|| [],
          chartType = EXPORTS.getChartConfType(options.dataType) || [],
          dateKey = 'timestamp,',
          sqlBase = 'SELECT %(date)s sum(push_click_user) as push_click_user,sum(back_list_user) as back_list_user,sum(impression) as impression ,sum(click) as click ,sum(dwelltime) as dwelltime FROM %(table)s WHERE %(where)s timestamp BETWEEN "%(start_time)s" AND  "%(end_time)s"',
          sqlGroupBy = 'SELECT %(date)s %(groupby)s,sum(push_click_user) as push_click_user,sum(back_list_user) as back_list_user,sum(impression) as impression ,sum(click) as click ,sum(dwelltime) as dwelltime FROM %(table)s WHERE %(where)s timestamp BETWEEN "%(start_time)s" AND  "%(end_time)s"  GROUP BY %(groupby)s',
          sqlTemplate = ((options.groupBy|| (groupbys.length != 0&&chartType != 'table_only')) && sqlGroupBy || sqlBase) + '%(orderby)s',
          sql;

      if (options.granularity==='all') dateKey = '';

      sql = sf.sprintf(sqlTemplate, {
        date: dateKey,
        metrics: local._getMetrics(options),
        where: local._getWhere(options, dataSource),
        groupby:local._getGroupBys(options),
        orderby:local._getOrderBy(options, dataSource),
        table: dataSource['source'],
        start_time: startTime,
        end_time: endTime
      });
      return sql;
    },

    constructMultiServiceUserSql: function(options){
      var startTime = global.moment(new Date(options.startTime)).format('YYYY-MM-DD HH:00:00'),
          endTime = global.moment(new Date(options.endTime)).format('YYYY-MM-DD HH:00:00'),
          dataSource = EXPORTS.getChartDataSourceConf(options.dataType)['mysql_nultiservice'],
          groupbys = EXPORTS.getChartGroupbyConf(options.dataType)|| [],
          chartType = EXPORTS.getChartConfType(options.dataType) || [],
          dateKey = "timestamp ",
          sqlBase = 'SELECT %(date)s, sum(multiservice_user) as multiservice_user FROM %(table)s WHERE %(where)s %(date)s BETWEEN "%(start_time)s" AND  "%(end_time)s" ',
          sqlTemplate = ((options.groupBy|| (groupbys.length != 0&&chartType != 'table_only')) && sqlGroupBy || sqlBase) + 'group by timestamp order by timestamp desc',
          sql;
      if (options.granularity==='all') dateKey = '';
      sql = sf.sprintf(sqlTemplate, {
        date: dateKey,
        metrics: local._getMetrics(options),
        where: local._getWhere(options, dataSource),
        groupby:local._getGroupBys(options),
        orderby:local._getOrderBy(options, dataSource),
        table: dataSource['source'],
        start_time: startTime,
        end_time: endTime
      });
      return sql;
    }, 

    constructPushUser:function(options){
      var startTime = global.moment(new Date(options.startTime)).format('YYYY-MM-DD HH:00:00').toString(),
          endTime = global.moment(new Date(options.endTime)).format('YYYY-MM-DD HH:00:00').toString(),
          dataSource = EXPORTS.getChartDataSourceConf(options.dataType)['mysql_push'],
          groupbys = EXPORTS.getChartGroupbyConf(options.dataType)|| [],
          chartType = EXPORTS.getChartConfType(options.dataType) || [],
          dateKey = 'push_time,',
          sqlBase = 'SELECT %(date)s invalid_new_user_num,failed_num,succeed_num,new_user_between_24hours FROM %(table)s WHERE %(where)s  push_time BETWEEN "%(start_time)s" AND  "%(end_time)s" ',
          sqlGroupBy = 'SELECT %(date)s %(groupby)s,invalid_new_user_num,failed_num,succeed_num,new_user_between_24hours FROM %(table)s WHERE %(where)s push_time BETWEEN "%(start_time)s" AND  "%(end_time)s" GROUP BY %(groupby)s',
          sqlTemplate = ((options.groupBy|| (groupbys.length != 0&&chartType != 'table_only')) && sqlGroupBy || sqlBase) + ' order by push_time desc',
          sql;

      if (options.granularity==='all') dateKey = '';

      sql = sf.sprintf(sqlTemplate, {
        date: dateKey,
        metrics: local._getMetrics(options),
        where: local._getWhere(options, dataSource),
        groupby:local._getGroupBys(options),
        orderby:local._getOrderBy(options, dataSource),
        table: dataSource['source'],
        start_time: startTime,
        end_time: endTime
      });
      return sql;
    },

     constructMonitorSql:function(options){
          var startTime = global.moment(new Date(options.startTime)).format('YYYY-MM-DD HH:00:00').toString(),
              endTime = global.moment(new Date(options.endTime)).format('YYYY-MM-DD HH:00:00').toString(),
              dataSource = EXPORTS.getChartDataSourceConf(options.dataType)['mysql6'],
              groupbys = EXPORTS.getChartGroupbyConf(options.dataType)|| [],
              chartType = EXPORTS.getChartConfType(options.dataType) || [],
              dateKey = 'timestamp,',
              sqlBase = 'SELECT timestamp,sum(crawler_amount) as crawler_amount,sum(publish_amount) as publish_amount FROM %(table)s WHERE %(where)s timestamp BETWEEN "%(start_time)s" AND  "%(end_time)s" GROUP BY timestamp ',
              sqlGroupBy = 'SELECT %(date)s %(groupby)s, %(metrics)s FROM %(table)s WHERE %(where)s timestamp>="%(start_time)s" AND timestamp<"%(end_time)s" GROUP BY %(groupby)s',
              sqlTemplate = ((options.groupBy|| (groupbys.length != 0&&chartType != 'table_only')) && sqlGroupBy || sqlBase) + ' order by timestamp asc limit 1000',
              sql;

          if (options.granularity==='all') dateKey = '';

          sql = sf.sprintf(sqlTemplate, {
              date: dateKey,
              metrics: local._getMetrics(options),
              where: local._getWhere(options, dataSource),
              groupby:local._getGroupBys(options),
              orderby:local._getOrderBy(options, dataSource),
              table: dataSource['source'],
              start_time: startTime,
              end_time: endTime
          });
          return sql;
      },    

     constructArticleDailySql:function(options){
          var startTime = global.moment(new Date(options.startTime)).format('YYYY-MM-DD HH:00:00').toString(),
              endTime = global.moment(new Date(options.endTime)).format('YYYY-MM-DD HH:00:00').toString(),
              dataSource = EXPORTS.getChartDataSourceConf(options.dataType)['mysql4'],
              groupbys = EXPORTS.getChartGroupbyConf(options.dataType)|| [],
              chartType = EXPORTS.getChartConfType(options.dataType) || [],
              dateKey = 'timestamp,',
              sqlBase = 'SELECT %(date)s sum(request) as request,sum(impression) as impression,sum(click) as click,sum(dwelltime) as dwelltime FROM %(table)s WHERE %(where)s ',
              sqlGroupBy = 'SELECT %(date)s %(groupby)s,sum(request) as request,sum(impression) as impression,sum(click) as click,sum(dwelltime) as dwelltime FROM %(table)s WHERE %(where)s timestamp BETWEEN "%(start_time)s" AND  "%(end_time)s"  GROUP BY %(groupby)s',
              sqlTemplate = ((options.groupBy|| (groupbys.length != 0&&chartType != 'table_only')) && sqlGroupBy || sqlBase) + '%(orderby)s limit 3000',
              sql;

          if (options.granularity==='all') dateKey = '';

          sql = sf.sprintf(sqlTemplate, {
              date: dateKey,
              metrics: local._getMetrics(options),
              where: local._getWhere(options, dataSource),
              groupby:local._getGroupBys(options),
              orderby:local._getOrderBy(options, dataSource),
              table: dataSource['source']+'_'+global.moment(new Date(options.startTime)).format('YYYYMMDD').toString(),
              start_time: startTime,
              end_time: endTime
          });
          return sql;
      },  

    constructArticleDailyOperateSql:function(options){
          var startTime = global.moment(new Date(options.startTime)).format('YYYY-MM-DD HH:00:00').toString(),
              endTime = global.moment(new Date(options.endTime)).format('YYYY-MM-DD HH:00:00').toString(),
              dataSource = EXPORTS.getChartDataSourceConf(options.dataType)['mysql_operate'],
              groupbys = EXPORTS.getChartGroupbyConf(options.dataType)|| [],
              chartType = EXPORTS.getChartConfType(options.dataType) || [],
              dateKey = 'timestamp,',
              sqlBase = 'SELECT %(date)s sum(request) as request,sum(impression) as impression,sum(click) as click,sum(dwelltime) as dwelltime FROM %(table)s WHERE %(where)s ',
              sqlGroupBy = 'SELECT %(date)s %(groupby)s,sum(request) as request,sum(impression) as impression,sum(click) as click,sum(dwelltime) as dwelltime FROM %(table)s WHERE %(where)s timestamp BETWEEN "%(start_time)s" AND  "%(end_time)s"  GROUP BY %(groupby)s',
              sqlTemplate = ((options.groupBy|| (groupbys.length != 0&&chartType != 'table_only')) && sqlGroupBy || sqlBase) + '%(orderby)s limit 3000',
              sql;

          if (options.granularity==='all') dateKey = '';

          sql = sf.sprintf(sqlTemplate, {
              date: dateKey,
              metrics: local._getMetrics(options),
              where: local._getWhere(options, dataSource),
              groupby:local._getGroupBys(options),
              orderby:local._getOrderBy(options, dataSource),
              table: dataSource['source']+'_'+global.moment(new Date(options.startTime)).format('YYYYMMDD').toString(),
              start_time: startTime,
              end_time: endTime
          });
          return sql;
      },

    constructArticleDailySumSql:function(options){
      var startTime = global.moment(new Date(options.startTime)).format('YYYY-MM-DD HH:00:00').toString(),
          endTime = global.moment(new Date(options.endTime)).format('YYYY-MM-DD HH:00:00').toString(),
          dataSource = EXPORTS.getChartDataSourceConf(options.dataType)['mysql_operate'],
          groupbys = EXPORTS.getChartGroupbyConf(options.dataType)|| [],
          chartType = EXPORTS.getChartConfType(options.dataType) || [],
          dateKey = 'timestamp,',
          sqlBase = 'SELECT sum(request) as request_sum,sum(impression) as impression_sum,sum(click) as click_sum,sum(dwelltime) as dwelltime_sum FROM %(table)s WHERE %(where)s ',
          sqlGroupBy = 'SELECT sum(request) as request_sum,sum(impression) as impression_sum,sum(click) as click_sum,sum(dwelltime) as dwelltime_sum FROM %(table)s WHERE %(where)s timestamp>="%(start_time)s" AND timestamp<"%(end_time)s"',
          sqlTemplate = ((options.groupBy|| (groupbys.length != 0&&chartType != 'table_only')) && sqlGroupBy || sqlBase),
          sql;

      if (options.granularity==='all') dateKey = '';

      sql = sf.sprintf(sqlTemplate, {
        date: dateKey,
        metrics: local._getMetrics(options),
        where: local._getWhere(options, dataSource),
        groupby:local._getGroupBys(options),
        orderby:local._getOrderBy(options, dataSource),
        table: dataSource['source']+'_'+global.moment(new Date(options.startTime)).format('YYYYMMDD').toString(),
        start_time: startTime,
        end_time: endTime
      });
      return sql;
    },

    constructUserAge:function(options){

      var startTime = global.moment(new Date(options.startTime)).format('YYYY-MM-DD HH:00:00').toString(),
                endTime = global.moment(new Date(options.endTime)).format('YYYY-MM-DD HH:00:00').toString(),
                dataSource = EXPORTS.getChartDataSourceConf(options.dataType)['mysql1'],
                groupbys = EXPORTS.getChartGroupbyConf(options.dataType)|| [],
                chartType = EXPORTS.getChartConfType(options.dataType) || [],
                dateKey = 'timestamp,',
                sqlBase = 'SELECT timestamp,sum(service_user) as service_user,sum(activity_user) as activity_user,sum(total_request) as total_request,sum(total_request_user) as total_request_user,sum(total_impression) as total_impression,sum(total_impression_user) as total_impression_user,sum(total_click) as total_click,sum(total_click_user) as total_click_user,sum(total_detail_dwell_time) as total_detail_dwell_time,sum(total_listpage_dwell_time as total_listpage_dwell_time FROM %(table)s WHERE %(where)s timestamp BETWEEN "%(start_time)s" AND  "%(end_time)s" ',
                sqlGroupBy = 'SELECT timestamp, %(groupby)s,sum(service_user) as service_user,sum(activity_user) as activity_user,sum(total_request) as total_request,sum(total_request_user) as total_request_user,sum(total_impression) as total_impression,sum(total_impression_user) as total_impression_user,sum(total_click) as total_click,sum(total_click_user) as total_click_user,sum(total_detail_dwell_time) as total_detail_dwell_time,sum(total_listpage_dwell_time) as total_listpage_dwell_time FROM %(table)s WHERE %(where)s timestamp>="%(start_time)s" AND timestamp<"%(end_time)s" GROUP BY %(groupby)s',
                sqlTemplate = ((options.groupBy|| (groupbys.length != 0&&chartType != 'table_only')) && sqlGroupBy || sqlBase) + '%(orderby)s',
                sql;

            if (options.granularity==='all') dateKey = '';

            sql = sf.sprintf(sqlTemplate, {
                date: dateKey,
                metrics: local._getMetrics(options),
                where: local._getWhere(options, dataSource),
                groupby:local._getGroupBys(options),
                orderby:local._getOrderBy(options, dataSource),
                table: dataSource['source'],
                start_time: startTime,
                end_time: endTime
            });
        return sql;
    },

    constructPush:function(options){
          var startTime = global.moment(new Date(options.startTime)).format('YYYY-MM-DD HH:00:00'),
              endTime = global.moment(new Date(options.endTime)).format('YYYY-MM-DD HH:00:00'),
              dataSource = EXPORTS.getChartDataSourceConf(options.dataType)['mysql11'],
              groupbys = EXPORTS.getChartGroupbyConf(options.dataType)|| [],
              chartType = EXPORTS.getChartConfType(options.dataType) || [],
              dateKey = 'timestamp,',
              sqlBase = 'SELECT %(date)s sum(activity_before_click_user) as activity_before_click_user,sum(push_active_user) as push_active_user,sum(service_user) as service_user,sum(impression_push_user) as impression_push_user,sum(click_push_user) as click_push_user,sum(activity_user) as activity_user,sum(service_push_shut_user) as service_push_shut_user FROM %(table)s WHERE %(where)s timestamp BETWEEN  "%(start_time)s" AND  "%(end_time)s" ',
              sqlGroupBy = 'SELECT %(date)s %(groupby)s, %(metrics)s FROM %(table)s WHERE %(where)s timestamp>="%(start_time)s" AND timestamp<"%(end_time)s" GROUP BY %(groupby)s',
              sqlTemplate = ((options.groupBy|| (groupbys.length != 0&&chartType != 'table_only')) && sqlGroupBy || sqlBase) + ' group by timestamp  order by timestamp asc',
              sql;
          if (options.granularity==='all') dateKey = '';

          sql = sf.sprintf(sqlTemplate, {
              date: dateKey,
              metrics: local._getMetrics(options),
              where: local._getWhere(options, dataSource),
              groupby:local._getGroupBys(options),
              orderby:local._getOrderBy(options, dataSource),
              table: dataSource['source'],
              start_time: startTime,
              end_time: endTime
          });
          return sql;
      },
 
    constructNewUserMonitor:function(options){
          var startTime = global.moment(new Date(options.startTime)).format('YYYY-MM-DD HH:00:00'),
              endTime = global.moment(new Date(options.endTime)).format('YYYY-MM-DD HH:00:00'),
              dataSource = EXPORTS.getChartDataSourceConf(options.dataType)['mysql7'],
              groupbys = EXPORTS.getChartGroupbyConf(options.dataType)|| [],
              chartType = EXPORTS.getChartConfType(options.dataType) || [],
              dateKey = 'timestamp,',
              //sqlBase = 'SELECT %(date)s new_user,application_user,activity_user,impression_user,click_user FROM %(table)s WHERE %(where)s timestamp BETWEEN "%(start_time)s" AND  "%(end_time)s" ',
              //sqlTemplate = ((options.groupBy|| (groupbys.length != 0&&chartType != 'table_only')) && sqlGroupBy || sqlBase) + ' group by timestamp  order by timestamp desc',
              sqlBase = 'SELECT %(date)s new_user,mainhome_activity_user,loading_activity_user,impression_all_user,long_listpage_impression_user,application_user,activity_user,impression_user,click_user FROM %(table)s WHERE %(where)s timestamp BETWEEN "%(start_time)s" AND  "%(end_time)s" group by timestamp ',
              sqlGroupBy = 'SELECT %(date)s %(groupby)s, new_user,mainhome_activity_user,loading_activity_user,impression_all_user,long_listpage_impression_user,application_user,activity_user,impression_user,click_user FROM %(table)s WHERE %(where)s timestamp>="%(start_time)s" AND timestamp<"%(end_time)s" GROUP BY timestamp, %(groupby)s',
              sqlTemplate = ((options.groupBy|| (groupbys.length != 0&&chartType != 'table_only')) && sqlGroupBy || sqlBase) + ' order by timestamp desc',
              sql;
          if (options.granularity==='all') dateKey = '';

          sql = sf.sprintf(sqlTemplate, {
              date: dateKey,
              metrics: local._getMetrics(options),
              where: local._getWhere(options, dataSource),
              groupby:local._getGroupBys(options),
              orderby:local._getOrderBy(options, dataSource),
              table: dataSource['source'],
              start_time: startTime,
              end_time: endTime
          });
          return sql;
      },

    constructThroughput:function(options){
            var startTime = global.moment(new Date(options.startTime)).format('YYYY-MM-DD HH:00:00').toString(),
                endTime = global.moment(new Date(options.endTime)).format('YYYY-MM-DD HH:00:00').toString(),
                dataSource = EXPORTS.getChartDataSourceConf(options.dataType)['mysql13'],
                groupbys = EXPORTS.getChartGroupbyConf(options.dataType)|| [],
                chartType = EXPORTS.getChartConfType(options.dataType) || [],
                dateKey =  options.granularity,
                //sqlBase = 'SELECT P1.%(date1)s, sum(article_throughput) as article_throughput,sum(article_available_amount) as article_available_amount FROM (select %(date1)s,sum(article_available_amount) AS article_available_amount from t_article_publish P where %(where)s %(date)s>="%(start_time)s" AND %(date)s<"%(end_time)s" GROUP BY  %(date)s ) P1 left join ( select %(date1)s,sum(article_throughput) AS article_throughput from  t_article_throughput P where %(where)s %(date)s>="%(start_time)s" AND %(date)s<"%(end_time)s" GROUP BY  %(date)s) P2 on P1.%(date1)s = P2.%(date1)s group by P1.%(date1)s',
                sqlBase = 'SELECT %(date)s,sum(article_throughput) as article_throughput FROM %(table)s WHERE %(where)s %(date)s BETWEEN "%(start_time)s" AND  "%(end_time)s" ',
                //dateKey = options.granularity,
                //sqlBase = 'SELECT %(date)s, sum(article_throughput) as article_throughput FROM %(table)s WHERE %(where)s %(date)s BETWEEN "%(start_time)s" AND  "%(end_time)s" ',
                //sqlTemplate = ((options.groupBy|| (groupbys.length != 0&&chartType != 'table_only')) && sqlGroupBy || sqlBase) + ' order by P1.%(date1)s asc',
                sqlTemplate = ((options.groupBy|| (groupbys.length != 0&&chartType != 'table_only')) && sqlGroupBy || sqlBase) + ' group by %(date)s order by %(date)s asc',
                sql;

            if (options.granularity==='all') dateKey = '';

            sql = sf.sprintf(sqlTemplate, {
                //date: 'P.' + dateKey,
                //date1:dateKey,
                date: dateKey,
                metrics: local._getMetrics(options),
                where: local._getWhere(options, dataSource),
                groupby:local._getGroupBys(options),
                orderby:local._getOrderBy(options, dataSource),
                table: dataSource['source'],
                start_time: startTime,
                end_time: endTime
            });
            return sql;
    },

    constructPublish:function(options){
            var startTime = global.moment(new Date(options.startTime)).format('YYYY-MM-DD HH:00:00').toString(),
                endTime = global.moment(new Date(options.endTime)).format('YYYY-MM-DD HH:00:00').toString(),
                dataSource = EXPORTS.getChartDataSourceConf(options.dataType)['mysql14'],
                groupbys = EXPORTS.getChartGroupbyConf(options.dataType)|| [],
                chartType = EXPORTS.getChartConfType(options.dataType) || [],
                dateKey =  options.granularity,
                sqlBase = 'SELECT %(date)s, sum(article_available_amount) as article_available_amount FROM %(table)s WHERE %(where)s %(date)s BETWEEN "%(start_time)s" AND  "%(end_time)s" ',
                sqlTemplate = ((options.groupBy|| (groupbys.length != 0&&chartType != 'table_only')) && sqlGroupBy || sqlBase) + ' group by %(date)s order by %(date)s asc',
                sql;

            if (options.granularity==='all') dateKey = '';

            sql = sf.sprintf(sqlTemplate, {
                date: dateKey,
                date1:dateKey,
                metrics: local._getMetrics(options),
                where: local._getWhere(options, dataSource),
                groupby:local._getGroupBys(options),
                orderby:local._getOrderBy(options, dataSource),
                table: dataSource['source'],
                start_time: startTime,
                end_time: endTime
            });
            return sql;
    },

    constructFeederSql: function(options){
            var startTime = global.moment(new Date(options.startTime)).format('YYYY-MM-DD HH:00:00'),
                endTime = global.moment(new Date(options.endTime)).format('YYYY-MM-DD HH:00:00'),
                dataSource = EXPORTS.getChartDataSourceConf(options.dataType)['mysql5'],
                groupbys = EXPORTS.getChartGroupbyConf(options.dataType)|| [],
                chartType = EXPORTS.getChartConfType(options.dataType) || [],
                dateKey = options.granularity,
                sqlBase = 'SELECT %(date)s,article_available_amount FROM %(table)s WHERE %(where)s %(date)s BETWEEN "%(start_time)s" AND  "%(end_time)s" ',
                sqlGroupBy = 'SELECT %(date)s %(groupby)s, %(metrics)s FROM %(table)s WHERE %(where)s %(date)s>="%(start_time)s" AND %(date)s<"%(end_time)s" GROUP BY %(groupby)s',
                sqlTemplate = ((options.groupBy|| (groupbys.length != 0&&chartType != 'table_only')) && sqlGroupBy || sqlBase) + ' group by %(date)s %(orderby)s',
                sql;
            if (options.granularity==='all') dateKey = '';
            sql = sf.sprintf(sqlTemplate, {
                date: dateKey,
                metrics: local._getMetrics(options),
                where: local._getWhere(options, dataSource),
                groupby:local._getGroupBys(options),
                orderby:local._getOrderBy(options, dataSource),
                table: dataSource['source'],
                start_time: startTime,
                end_time: endTime
            });
            return sql;
    },

    constructUserTable:function(options){
            var startTime = global.moment(new Date(options.startTime)).format('YYYY-MM-DD HH:00:00').toString(),
                endTime = global.moment(new Date(options.endTime)).format('YYYY-MM-DD HH:00:00').toString(),
                dataSource = EXPORTS.getChartDataSourceConf(options.dataType)['mysql_table'],
                groupbys = EXPORTS.getChartGroupbyConf(options.dataType)|| [],
                chartType = EXPORTS.getChartConfType(options.dataType) || [],
                dateKey = 'timestamp,',
                sqlBase = 'SELECT sum(total_request) as total_request,sum(total_request_user) as total_request_user,sum(total_impression) as total_impression,sum(total_impression_user) as total_impression_user,sum(total_click) as total_click,sum(total_click_user) as total_click_user,sum(total_detail_dwell_time) as total_detail_dwell_time,sum(total_listpage_dwelltime) as total_listpage_dwelltime,sum(total_listpage_dwelltime_user) as total_listpage_dwelltime_user FROM %(table)s WHERE %(where)s timestamp>="%(start_time)s" AND  timestamp<"%(end_time)s" ',
                sqlGroupBy = 'SELECT %(date)s %(groupby)s,sum(total_request) as total_request,sum(total_request_user) as total_request_user,sum(total_impression) as total_impression,sum(total_impression_user) as total_impression_user,sum(total_click) as total_click,sum(total_click_user) as total_click_user,sum(total_detail_dwell_time) as total_detail_dwell_time,sum(total_listpage_dwelltime) as total_listpage_dwelltime,sum(total_listpage_dwelltime_user) as total_listpage_dwelltime_user FROM %(table)s WHERE %(where)s timestamp>="%(start_time)s" AND timestamp<"%(end_time)s" GROUP BY %(date)s  %(groupby)s',
                sqlTemplate = ((options.groupBy|| (groupbys.length != 0&&chartType != 'table_only')) && sqlGroupBy || sqlBase) + ' order by timestamp asc',
                sql;

            if (options.granularity==='all') dateKey = '';

            sql = sf.sprintf(sqlTemplate, {
                date: dateKey,
                metrics: local._getMetrics(options),
                where: local._getWhere(options, dataSource),
                groupby:local._getGroupBys(options),
                orderby:local._getOrderBy(options, dataSource),
                table: dataSource['source'],
                start_time: startTime,
                end_time: endTime
            });
            return sql;
    },

    constructUserHistogramSql:function(options){
            var startTime = global.moment(new Date(options.startTime)).format('YYYY-MM-DD HH:00:00').toString(),
                endTime = global.moment(new Date(options.endTime)).format('YYYY-MM-DD HH:00:00').toString(),
                dataSource = EXPORTS.getChartDataSourceConf(options.dataType)['mysql_user_histogram'],
                groupbys = EXPORTS.getChartGroupbyConf(options.dataType)|| [],
                chartType = EXPORTS.getChartConfType(options.dataType) || [],
                dateKey = 'timestamp,',
                sqlTemplate = 'select timestamp,num,sum(user_count) as user_count FROM %(table)s WHERE %(where)s timestamp>="%(start_time)s" AND timestamp<"%(end_time)s"  group by num',
                sql;

            if (options.granularity==='all') dateKey = '';

            sql = sf.sprintf(sqlTemplate, {
                date: dateKey,
                metrics: local._getMetrics(options),
                where: local._getWhere(options, dataSource),
                groupby:local._getGroupBys(options),
                orderby:local._getOrderBy(options, dataSource),
                table: dataSource['source'],
                start_time: startTime,
                end_time: endTime
            });
            return sql;
    },


    constructSql: function(options){
      var startTime = global.moment(new Date(options.startTime)).format('YYYY-MM-DD HH:00:00'),
        endTime = global.moment(new Date(options.endTime)).format('YYYY-MM-DD HH:00:00'),
        dataSource = EXPORTS.getChartDataSourceConf(options.dataType)['mysql'],
        groupbys = EXPORTS.getChartGroupbyConf(options.dataType)|| [],
        chartType = EXPORTS.getChartConfType(options.dataType) || [],
        dateKey = 'data,',
        sqlBase = 'SELECT %(date)s %(metrics)s FROM %(table)s WHERE %(where)s DimKey = "%(dimKey)s" AND create_time BETWEEN "%(start_time)s" AND  "%(end_time)s" ',
        sqlGroupBy = 'SELECT %(date)s %(groupby)s, %(metrics)s FROM %(table)s WHERE %(where)s DimKey = "%(dimKey)s" AND Date>="%(start_time)s" AND Date<"%(end_time)s" GROUP BY %(groupby)s',
        sqlTemplate = ((options.groupBy|| (groupbys.length != 0&&chartType != 'table_only')) && sqlGroupBy || sqlBase) + '%(orderby)s',
        sql;
      if (options.granularity==='all') dateKey = '';
      sql = sf.sprintf(sqlTemplate, {
        date: dateKey,
        dimKey: local._getDimKey(options, dataSource),
        metrics: local._getMetrics(options),
        where: local._getWhere(options, dataSource),
        groupby:local._getGroupBys(options),
        orderby:local._getOrderBy(options, dataSource),
        table: dataSource['source'],
        start_time: startTime,
        end_time: endTime
      });
      return sql;
    },

    constructEventInsertSql:function(req,paramId){
      var insertSql = 'insert into %(table)s (%(metrics)s) values (%(value)s)',
        sqlTemplate = insertSql,
        dataSource = EXPORTS.getChartDataSourceConf(paramId)['mysql'],
        metricsStrArr = [],
        metricsValue = [],
        sql;
        for(var i in req){
          metricsStrArr.push(i);
          metricsValue.push('"'+req[i]+'"');
        }
      sql = sf.sprintf(sqlTemplate,{
        table:dataSource['source'],
        metrics:metricsStrArr.join(','),
        value:metricsValue.join(',')
      });
      return sql;
    },

    constructEventSql:function(req){
      var startTime = global.moment(new Date(req.startTime)).format('YYYY-MM-DD HH:00:00'),
        endTime = global.moment(new Date(req.endTime)).format('YYYY-MM-DD HH:00:00'),
        impact_scope = req.impact_scope || null,
        querySql = 'select %(metrics)s from %(table)s where %(impact_scope)s   event_time between "%(start_time)s" and "%(end_time)s" ',
        sqlTemplate = querySql,
        dataSource = EXPORTS.getChartDataSourceConf(req.dataType)['mysql'],
        metricsArray = EXPORTS.getChartMetricsConf(req.dataType, 'mysql') || [],
        metricsStrArr = [],
        sql;
      metricsArray.forEach(function(m){
        var mname = m && m.name ;
        metricsStrArr.push(mname);
      });
      sql = sf.sprintf(sqlTemplate,{
        table:dataSource['source'],
        metrics:metricsStrArr.join(','),
        start_time: startTime,
        end_time: endTime,
        impact_scope: impact_scope != null ? 'impact_scope = "'+impact_scope+'" and':''
      });
      return sql;
    },


   requestOverallData: function(sql, conn,cb, req) {
      console.log('SQL Request:\n' + sql);
      conn.query(sql, function (err, data) {
        var formatData = [],

        data = data || [];
        try {
          data.forEach(function(d) {
            var  testData = {};
              testData['timestamp'] = d.Date|| d.date;
              testData['result'] = d;
              formatData.push(testData);
          });

        } catch (e) {
          err = e;
        }
        cb(err, formatData);
      });
        conn.end();
    },

    requestMysqlData: function(sql, cb, req) {
      console.log('SQL Request:\n' + sql);
      req.models.db.driver.execQuery(sql, function (err, data) {
        var formatData = [],

          data = data || [];

        try {
          data.forEach(function(d) {
            var  testData = {};
              testData['timestamp'] = d.Date|| d.date;
              testData['result'] = d;
              formatData.push(testData);
          });

        } catch (e) {
          err = e;
        }
        cb(err, formatData);
      });
    }

  };

  local._init();
}());
