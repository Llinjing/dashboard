(function moduleQueryNodejs() {
  /*
    this module exports request related methods (http, https)
  */
  'use strict';

  var local = {

    _name: 'request.moduleQueryNodejs',

    _init: function () {
      EXPORTS.moduleInit(typeof module === 'object' ? module : null, local);
    },

    getFilterCombinationList: function(cond) {
      var filterList = [],
        filterKey = [],
        combinationList = [],
        newFilterList = [],
        i;

      // Map [[1,2,3], [22,33], [111, 222]] to [[1,22,111], [1,22, 222] ...]
      function mapCombina(arr, x, str, len) {
        var y, news;
        for (y = 0; y < arr[x].length; y++) {
          news = str ? (str + '\t' + arr[x][y]) : arr[x][y];
          if (x === len - 1) {
            //console.log(news);
            combinationList.push(news.split('\t'));
          } else {
            mapCombina(arr, x + 1, news, len);
          }
        }
      }

      if (_.isEmpty(cond)) {
        return [{}];
      }

      for (i in cond) {
        filterList.push(cond[i].values);
        filterKey.push(i);
      }

      mapCombina(filterList, 0, '', filterList.length);

      combinationList.forEach(function(combin) {
        var nfilter = {};
        for (i = 0; i < combin.length; i++) {
          nfilter[filterKey[i]] = {
            "type": "in",
            "values": [
              combin[i]
            ]
          };
        }
        newFilterList.push(nfilter);
      });

      return newFilterList;
    }


  };

  local._init();
}());
