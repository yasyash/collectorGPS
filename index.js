

const express = require('express');
const bodyParser = require('body-parser');
const query = require('./api/query');
const injector = require('./api/injector');
const try_gps = require('./api/injector');

const cron = require('node-cron');
const https = require('https');
const fs = require('fs');
const superagent = require('superagent');

const app = express();

app.use(bodyParser.json());
app.use('/query', query);

const options = {
  //key: fs.readFileSync('./keys/chel_key.key'),
  //cert: fs.readFileSync('./keys/asoiza.voeikovmgo.crt')
};


https.createServer(options, app).listen(8383, () => {

  console.log('Client SSL is started on 8383 port...');
  cron.schedule("* * * * *", () => {
    console.log("try gps begin");
    try_gps();
  });

});

