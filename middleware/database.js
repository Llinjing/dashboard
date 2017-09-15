var orm = require('orm'),
  database = state.env && state.env.database || {};


module.exports = orm.express("mysql://" + database.user + ":" + database.password + "@" + database.host + (database.port ? (":" + database.port) : "") + "/" + database.database, {
  define: function (db, models, next) {
    models.db = db;
    next();
  }
});