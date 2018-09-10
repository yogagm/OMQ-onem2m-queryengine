const http = require('http');
const bodyParser = require('body-parser');
const xmlparser = require('express-xml-bodyparser');
const express = require('express'),
app = module.exports.app =  express();

var webServer = http.createServer(app);;
app.use(bodyParser.json());
app.use(xmlparser());

var amqp = require('amqplib/callback_api');
amqp.connect('amqp://localhost', function(err, conn) {
    conn.createChannel(function(err, ch) {
         ch.assertQueue('onem2mqe_local_data', {durable: false});
         app.post('/', function(req, res) {
            res.end("OK");

            sub = req.body.sgn.sur.split("/");
            thing = "/" + sub[1] + "/" + sub[2] + "/" + sub[3];
            value = parseFloat(req.body.sgn.nev.rep["m2m:cin"].con);

            timestamp = req.body.sgn.ts;

            output = {"type": 0, "source": thing, "timestamp": timestamp ,"data": value }
            ch.sendToQueue('onem2mqe_local_data', new Buffer(JSON.stringify(output)));
            console.log(JSON.stringify(output));
        });

        app.listen(1111)
     });
});

