

const express = require('express');
const bodyParser = require('body-parser');
const query = require('./api/query');
const injector = require('./api/injector');

const cron = require('node-cron');
const https = require('https');
const fs = require('fs');
const superagent = require('superagent');

const app = express();

app.use(bodyParser.json());
app.use('/query', query);

const options = {
  key: fs.readFileSync('./keys/chel_key.key'),
  cert: fs.readFileSync('./keys/chel_cert.crt')
};


https.createServer(options, app).listen(8383, () => {

  console.log('Server SSL is started on 8383 port...');
  cron.schedule("* * * * *", () => {
    console.log("injection");
    injector();
  });

});


