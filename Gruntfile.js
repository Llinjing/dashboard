module.exports = function (grunt) {

  function parseAssets() {
    var assets = grunt.file.readJSON('app/assets.json'),
      libFiles = {},
      jsModuleFiles = {},
      cssModuleFiles = {},
      i;

    for (i in assets) {
      if (i === 'lib') {
        libFiles['app/build/js/' + i + '.min.js'] = assets[i].js;
        libFiles['app/build/css/' + i + '.min.css'] = assets[i].css;
      } else {
        jsModuleFiles['app/build/js/' + i + '.min.js'] = assets[i].js;
        cssModuleFiles['app/build/css/' + i + '.min.css'] = assets[i].css;
      }
    }

    return {
      libFiles: libFiles,
      jsModuleFiles: jsModuleFiles,
      cssModuleFiles: cssModuleFiles
    };
  }

  grunt.initConfig({
    pkg: grunt.file.readJSON('package.json'),

    assets: parseAssets(),

    concat: {
      options: {
        separator: '\n\n\n'
      },
      build: {
        files: '<%= assets.libFiles %>'
      }
    },

    uglify: {
      options: {
        banner: '/*! <%= pkg.name %> <%= grunt.template.today("yyyy-mm-dd") %> */\n'
      },
      build: {
        files: '<%= assets.jsModuleFiles %>'
      }
    },

    cssmin: {
      combine: {
        files: '<%= assets.cssModuleFiles %>'
      }
    }
  });

  grunt.loadNpmTasks('grunt-contrib-uglify');
  grunt.loadNpmTasks('grunt-contrib-concat');
  grunt.loadNpmTasks('grunt-contrib-cssmin');

  grunt.registerTask('default', [
    'uglify',
    'concat',
    'cssmin'
  ]);

};
