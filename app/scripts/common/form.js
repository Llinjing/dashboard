/*
  this module includes actions of the dimension form
*/
define('common/form', function(exports) {
    'use strict';

    var initDimensionForm,
        getDimensionInfo,
        getGroupbyList,
        getShowMetricList,
        sendQuery,
        renderChartAndTable,
        rawData,
        drawOptions,
        dataUrl,
        getLabel,
        fixedExcelValue,
        excelDataRefactor,
        makeDownloadExcelButton,
        renderResultFunc;


    initDimensionForm = function() {
        var frm = $('form.dimensionForm', this),
            dateStartInput = $('.start input.datex', frm),
            dateEndInput = $('.end input.datex', frm),
            startBackDays = dateStartInput.data('backdays') || 7,
            endBackDays = dateEndInput.data('backdays') || 0,
            startTime =  moment().subtract(startBackDays, 'days').format('YYYY/MM/DD 00:00'),
            endTime =  moment().subtract(endBackDays, 'days').format('YYYY/MM/DD 24:00');
          
        //使dashboard支持分钟级的检索
        if(dataUrl.indexOf('table_only/top_impression')>0){
            startTime = moment().add('minutes',-10).format('YYYY/MM/DD HH:mm');
            endTime = moment().add('minutes',0).format('YYYY/MM/DD HH:mm');
        }
       
        if(dataUrl.indexOf('table_only/index_article_hours')>0 || dataUrl.indexOf('table_only/index_daily_h5share')>0){
            startTime = moment().add('minutes',-60).format('YYYY/MM/DD HH:00');
            endTime = moment().add('minutes',0).format('YYYY/MM/DD HH:00');
        }
       
        // init datetimepicker
        $('.datex', frm).datetimepicker({
            format: dateStartInput.data('format'),
            minDate: window.settings['data-start-time'] || '2015-05-24',
            maxDate: moment().add(90, 'days').format('YYYY-MM-DD')
        });

        // set start time and end time
        dateStartInput.data('DateTimePicker').date(startTime);
        dateEndInput.data('DateTimePicker').date(endTime);

        // init select2 select box
        $('.dimension-select', frm).each(function() {
            var self = $(this),
                initValue = self.data('defaults').toString().split(',');

            self.select2({
                theme: 'bootstrap',
                dropdownAutoWidth: true,
                tags: true,
                data: self.data('options'),
                matcher: function(params, data) {
                    // If there are no search terms, return all of the data
                    if ($.trim(params.term) === '') {
                        return data;
                    } else if (data.text.toLowerCase().indexOf(params.term.toLowerCase()) >= 0) {
                        return data;
                    } else {
                        return null;
                    }
                }
            });

            if (initValue[0]) {
                self.select2('val', initValue);
            }
        });

        $('form.metricForm input').change(function() {
            // render chart and table if there is cache data
            renderChartAndTable(rawData, drawOptions);
        });
        $('input[name="granularity"]', frm).change(function() {
            frm.submit();
        });
        $('input[name="dataSourceType"]', frm).change(function() {
            frm.submit();
        });

        frm.submit(function(event) {
            event.preventDefault();
            sendQuery(function(err, data) {
                renderChartAndTable(data, drawOptions);
            });
        });

        frm.submit();

        $('.toggleCharts').click(function() {
            $('.chart-container').toggle();
        });

    };

    sendQuery = function(callback) {
        var resultContainer = $('.chart-table-container'),
            queryForm = $('form.dimensionForm'),
            submitBtn = $('button[type=submit]', queryForm),
            queryFormArray = queryForm.serializeArray(),
            queryFilter = {},
            postBody,
            groupbyList;

        submitBtn.attr('disabled', 'disabled');
        resultContainer.hide();

        queryFormArray.forEach(function(q) {
            var dimension = q.name,
                val = q.value,
                initial = {
                    type: 'in',
                    values: []
                };

            if ((dimension.indexOf('-not') < 0) && (_.indexOf(['start_time', 'end_time', 'granularity','dataSourceType'], dimension) < 0)) {

                if (!queryFilter[dimension]) {
                    queryFilter[dimension] = initial;
                }

                queryFilter[dimension]['values'].push(val);

            }
        });

        // change 'not inclued' type to 'not'
        queryFormArray.forEach(function(q) {
            var dimension;
            if (q.name.indexOf('-not') >= 0 && q.value === 'on') {
                dimension = q.name.replace('-not', '');
                if (queryFilter[dimension]) {
                    queryFilter[dimension]['type'] = 'not';
                }
            }
        });

        drawOptions = getDimensionInfo(queryForm);
        groupbyList = getGroupbyList();
        postBody = {
            startTime: drawOptions.startTime,
            endTime: drawOptions.endTime,
            granularity: drawOptions.granularity,
            dataSourceType:drawOptions.dataSourceType,
            filter: queryFilter
        };

        if (groupbyList.length) {
            postBody['groupBy'] = groupbyList;
            drawOptions['groupBy'] = groupbyList;
        }

        drawOptions.postBody = postBody;
        Pace.track(function() {
            $.post(dataUrl, postBody).done(function(res) {
                submitBtn.removeAttr('disabled');
                if (res.status === 200) {
                    rawData = res.content;
                    resultContainer.show();
                    callback(null, rawData);
                    if(res.msg.indexOf("task")>-1){
                        alert(res.msg);
                    }else{
                        $.notify(res.msg, {
                            button: 'Confirm',
                            className: 'success',
                            autoHide: true
                        });
                    }
                    //$.notify(res.msg, {
                    //    className: 'success',
                    //    autoHide: true,
                    //    autoHideDelay: 1000
                    //});
                } else {
                    callback(res);
                    $.notify(res.msg, {
                        className: 'error',
                        autoHide: true
                    });
                }
            }).fail(function(err) {
                submitBtn.removeAttr('disabled');
                callback(err);
                $.notify('Request Failed!', {
                    className: 'error',
                    autoHide: true
                });
            });
        });
    };

    getGroupbyList = function() {
        var groupbyFrom = $('form.groupbyForm'),
            groupbyArray,
            groupbyList = [];

        if (groupbyFrom.length) {
            groupbyArray = groupbyFrom.serializeArray(),
                groupbyArray.forEach(function(m) {
                    groupbyList.push(m.value);
                });
        }

        return groupbyList;
    };

    getDimensionInfo = function(queryForm) {
        var drawOptions = {
            dataSourceType: $('input[name="dataSourceType"]:checked', queryForm).val(),
            granularity: $('input[name="granularity"]:checked', queryForm).val(),
            startTime: $('input[name="start_time"]', queryForm).val(),
            endTime: $('input[name="end_time"]', queryForm).val(),
            expids: $('select[name="expid"]', queryForm).val() || ['*']
        };

        return drawOptions;
    };

    getShowMetricList = function() {
        var metricsForm = $('form.metricForm'),
            metricArray = metricsForm.serializeArray(),
            metricList = [];

        metricArray.forEach(function(m) {
            var mnode = $('input[value="' + m.value + '"]', metricsForm);
            metricList.push({
                label: mnode.data('label'),
                subtitle: mnode.data('subtitle'),
                name: m.value,
                template: mnode.data('template')
            });
        });

        return metricList;
    };

    /* for csv */
    getLabel = function(key) {
        var
            result = '',
            pageKey = $('input[ value=' + key + ']');
        result = pageKey ? pageKey.attr('data-label') : key;
        return result;
    }

    fixedExcelValue = function(key) {
        var result;
        if (!isNaN(key * 1)) {
            result = (key * 1).toFixed(3) * 1;
        } else {
            result = key;
        }
        return result;
    }

    excelDataRefactor = function(excelData) {
        var result = [];
        function makeUpData(_data, data_key, sum) {
            _data.forEach(function(r) {
                var d = {},
                    data = r.result || r.event || r;
                if (data) {
                    //r.timestamp && (d['Timestamp'] = moment(r.timestamp).format('YYYY-MM-DDTHH:mm') + moment().tz("UTC").format('Z') );
                    r.timestamp && (d['Timestamp'] = r.timestamp );
                    sum && (d['Timestamp'] = sum);
                    data_key && (d['Data Key'] = data_key);
                    for (var i in data) {
                        if(i == 'content_id'){
                            d['ArticleID'] =fixedExcelValue($(data[i]).attr('title'));
                        }else if(i == 'title'){
                            d['Title'] =fixedExcelValue($(data[i]).attr('title'));
                            //d['Title'] =fixedExcelValue(data[i]);// modify by cc 20160822
                        }else{
                            if (i != 'Date' && i != 'timestamp') {
                                if(i=='url'){
                                    d['Url'] = fixedExcelValue(data[i]);
                                }else{
                                    d[getLabel(i)] = fixedExcelValue(data[i]);
                                }
                            }
                        }
                    }
                }
                result.push(d);
            });
        }
        if (excelData instanceof Array && excelData[0].result instanceof Array) {
            //makeUpData(excelData[0].result)// modify by cc 20160822
            var tempExcelData = [];
            for(var i=0;i<excelData.length;i++){
                tempExcelData = tempExcelData.concat(excelData[i].result);
            }
            makeUpData(tempExcelData)
        } else if (excelData instanceof Array && excelData.length > 0) {
            makeUpData(excelData)
        } else {
            for (var k in excelData) {
                var sum = [{
                    "result": excelData[k].sum
                }];
                var data = excelData[k].data;
                if (excelData[k].sum) {
                    makeUpData(sum, k, 'Summary');
                }
                if (data) {
                    makeUpData(data, k);
                }
            }
        }
        return result;
    }

    function JSONToCSVConvertor(JSONData, ReportTitle, ShowLabel) {
        var arrData = typeof JSONData != 'object' ? JSON.parse(JSONData) : JSONData;
        var CSV = '';
        var titles = [];
        CSV += ReportTitle.toString() + '\r\n\n';
        if (ShowLabel) {
            var row = "";
            for (var index in arrData[0]) {
                row += index + ',';
                titles.push(index);
            }
            row = row.slice(0, -1);
            CSV += row + '\r\n';
        
            for (var i = 0; i < arrData.length; i++) {
                var row = "";
                for (var index in titles) {
                    row += '"' + arrData[i][titles[index]] + '",';
                }
                row.slice(0, row.length - 1);
                CSV += row + '\r\n';
           }
        }else{
            for (var i = 0; i < arrData.length; i++) {
                var row = "";
                for (var index in arrData[i]) {
                    row += '"' + arrData[i][index] + '",';
                }
                row.slice(0, row.length - 1);
                CSV += row + '\r\n';
           }
        }

        if (CSV == '') {
            alert("Invalid data");
            return;
        }
        var fileName = "csv_"+new Date*1;
        
	//modify by cc 20160823 to fix tapd 1003040
	fileName += ReportTitle.toString().replace(/ /g, "_") +".csv";
        //var uri = 'data:text/csv;charset=gb2312,\ufeff' + encodeURIComponent(CSV);
        //var link = document.createElement("a");
        //link.href = uri;
        //link.setAttribute('style', 'visibility:hidden');
        //link.download = fileName + ".csv";
        //document.body.appendChild(link);
        //link.click();
        //document.body.removeChild(link);
        
        var aLink = document.createElement('a');
        var blob = new Blob(['\ufeff'+CSV], {type : 'text/csv,charset=UTF-8'});
        aLink.download = fileName;
        aLink.href = URL.createObjectURL(blob);
        aLink.click();
        window.URL.revokeObjectURL(aLink.href);
    }

    makeDownloadExcelButton = function(_data) {
        var downloadButton = $('<button id="downloadExcel" class="btn-sm btn-success download_bt" href="javascript:void(0)">Download Excel</button>')
        $('.dataTables_length').after(downloadButton);
        downloadButton.click(function() {
            var data = excelDataRefactor(_data);
            JSONToCSVConvertor(JSON.stringify(data), '', true);
        });
    };

    renderChartAndTable = function(data, drawOptions) {
        var metrics = getShowMetricList(),
            drawOptions = drawOptions || {};

        if (!data) {
            return;
        }

        drawOptions.metrics = metrics;
        drawOptions.data = data;
        renderResultFunc(drawOptions);
        makeDownloadExcelButton(data);
    };


    exports.init = function(options) {
        $(document).ready(initDimensionForm);
        dataUrl = options.dataUrl || '';
        renderResultFunc = options.renderResult;

    };

});
