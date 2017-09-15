var express = require('express'),
  ipfilter = require('express-ipfilter'),
  http = require('http'),
  path = require('path'),
  fs = require('fs'),
  favicon = require('serve-favicon'),
  logger = require('morgan'),
  compression = require('compression'),
  cookieParser = require('cookie-parser'),
  bodyParser = require('body-parser'),
  EXPORTS = global.EXPORTS = global.EXPORTS || {},
  state = global.state = global.state || {},
  required = global.required = EXPORTS.required = EXPORTS.required || {},
  envConf,
  app,
  server;

/* chdir to application directory */
process.chdir(state.__dirname = __dirname);

envConf = require('./env.json');

/* export hostname */
state.hostname = state.hostname
  || process.env.HOSTNAME
  || 'localhost';

state.env = envConf || {};

/* init utility module */
require('./local_modules/utility.js');
/* init other modules under local_modules */
fs.readdirSync(__dirname + '/local_modules/').forEach(function (m) {
  if (m !== 'utility.js') {
    require('./local_modules/' + m);
  }
});

/* Express */
app = express();
/*
if (state.hostname !== 'localhost') {
  app.use(ipfilter([
    'localhost',
    '172.31.4.52',
    '52.77.149.42',
    '52.221.239.146'
    ], {mode: 'allow'})
  );
}
*/
// view engine setup
app.set('views', path.join(__dirname, 'views'));
app.set('view engine', 'ejs');

app.use(compression());
app.use(bodyParser.json({limit: '50mb'}));
app.use(bodyParser.urlencoded({limit: '50mb', extended: true}));
app.use(cookieParser());



//mysql test
app.use('/data/table_only/:chart',require('./middleware/database.js'));
app.use('/data/chart_and_table/:chart',require('./middleware/database.js'));
app.use('/data/page_detail/:chart',require('./middleware/database.js'));
app.use('/data/histogram/:chart',require('./middleware/database.js')); // added by cc 20160919 直方图 


app.use(favicon(__dirname + '/app/favicon.ico'));
app.use(express.static(__dirname + '/app'));

// routes
fs.readdirSync(__dirname + '/routes/').forEach(function (m) {
  app.use(require('./routes/' + m));
});

// catch 404 and forward to error handler
app.use(function(req, res, next) {
  console.log(req && req.url);
  var err = new Error('Not Found');
  err.status = 404;
  next(err);
});

server = http.createServer(app).listen(state.env.port || 8182, function() {
  console.log('Server is listening...');
});

module.exports = server;
