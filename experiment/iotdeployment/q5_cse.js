const unirest = require('unirest');
const uuid = require('uuid/v4');
const express = require('express');
const bodyParser = require('body-parser');
const nj = require('numjs');

const ip = process.argv[2];


// Subscription
const subscribe_headers = {"X-M2M-Origin": "SOrigin", "X-M2M-RI": 12345, "Accept": "application/json", "Content-Type": "application/json;ty=23"};
const subscribe_body = {"m2m:sub": {"nu": ["http://" + ip], "nct":2 }}
const url = "http://192.168.0.100:8080";

unirest.post(url + "/mn-0/sysmon/mem_usage").headers(subscribe_headers).send(subscribe_body).end();

// Data retrieval

const app = express();
app.use(bodyParser.json());
app.post('/', function(req, res) {
    res.end("OK");
    sub = req.body.sgn.sur.split("/");
    src = "/" + sub[1] + "/" + sub[2] + "/" + sub[3];
    value = parseFloat(req.body.sgn.nev.rep["m2m:cin"].con);
    timestamp = req.body.sgn.ts;
    
    delay = (new Date()).getTime() - timestamp;
    start = (new Date()).getTime();
    if(value > 60) {
        //console.log({"temp": value});
        end = (new Date()).getTime();
        delay += (end-start);
        benchmark = {"delay": delay}
        console.log(JSON.stringify(benchmark));
    }
    
});

const port = parseInt(ip.split(":")[1])
app.listen(port);


