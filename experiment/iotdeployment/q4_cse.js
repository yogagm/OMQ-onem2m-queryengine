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

unirest.post(url + "/mn-0/sysmon/cpu_usage").headers(subscribe_headers).send(subscribe_body).end();
unirest.post(url + "/mn-1/sysmon/cpu_usage").headers(subscribe_headers).send(subscribe_body).end();
unirest.post(url + "/mn-2/sysmon/cpu_usage").headers(subscribe_headers).send(subscribe_body).end();
unirest.post(url + "/mn-3/sysmon/cpu_usage").headers(subscribe_headers).send(subscribe_body).end();

// Calculate results
var data = {};
var delay = [];

setInterval(function() {
    start = (new Date()).getTime();
    
    cpu_mean = []
    for(x in data) {
        cpu_mean.push(nj.mean(data[x]));
    }
    output = {"mean_cpu_usage": nj.mean(cpu_mean)}
    //console.log(output);
    
    end = (new Date()).getTime();
    mean_delay = nj.mean(delay);
    mean_delay += (end-start);
    benchmark = {"delay": mean_delay}
    console.log(JSON.stringify(benchmark));
    delay = []
}, 5000);

// Data retrieval
const app = express();
app.use(bodyParser.json());
app.post('/', function(req, res) {
    res.end("OK");
    sub = req.body.sgn.sur.split("/");
    src = "/" + sub[1] + "/" + sub[2] + "/" + sub[3];
    value = parseFloat(req.body.sgn.nev.rep["m2m:cin"].con);
    timestamp = req.body.sgn.ts;
    if(data[src] == undefined) {
        data[src] = [];
    }
    
    if(data[src].length >= 60) {
        data[src].shift();
        data[src].push(value);
    } else {
        data[src].push(value);
    }
    
    delay.push((new Date()).getTime() - timestamp);
});

const port = parseInt(ip.split(":")[1])
app.listen(port);


