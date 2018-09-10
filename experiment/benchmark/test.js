
// CONFIGURABLE - START
var n_queries = 1;
var n_test = 11;
var n_src = 10;
var target_mq = "amqp://127.0.0.1";
var types_of_query = "q11";
var time_between_test = 5000;
var time_warming_up = 5000;

// CONFIGURABLE - END


const spawn = require('child_process').spawn;
const uuid = require('uuid/v4');
const fs = require('fs');
const readline = require('readline');
const pickRandom = function(list) {
    idx = Math.floor(Math.random() * list.length)
    return list[idx];
}
const net = require('net');

var queries = [];
var dataset_location = "./trafficCombined.csv";
var ds_uris = JSON.parse(fs.readFileSync("./ds_uris.json"));


const chooseNearbyRoadId = function() {
    let maxFirstId = ds_uris.length - n_src
    let firstId = Math.floor(Math.random() * maxFirstId);
    let results = [];
    for(let i=firstId;i<firstId+n_src;i++) {
        results.push(ds_uris[i]);
    }
    
    return results;
}

for(i=0;i<n_queries;i++) {
    new_query = {"id": "query-" + i.toString(), "select": {}};

    // Source choice
    let containers = chooseNearbyRoadId();
    for(j=0;j<n_src;j++) {
        column_name = j.toString();
        new_select = {};
        
        containerName = containers[j]
        columnName = containerName.split("/")[1]

        new_select["_containers"] = containerName
        
        
        if(type_of_query == "q11") {
            new_select["1-aggregate"] = {
                "function": "sum",
                "output_rate": -1,
                "window_size": 13
            };
        }
        
        if(type_of_query == "q11" || type_of_query == "q12") {
            new_select["2-transform"] = {
                "function": "percentage",
                "args": [100]
            };
        }

        if(type_of_query == "q11" || type_of_query == "q12" || type_of_query == "q13") {
            new_select["3-filter"] = {
                "predicates": [columnName + ".vehicleCount>0"]
            };
        }

        new_query["select"][columnName] = new_select;
    }


    queries.push(new_query)
}


var amqp = require('amqplib/callback_api');
var sleep = require('sleep');
amqp.connect(targetMQ, function(err, conn) {
     conn.createChannel(function(err, ch) {
        ch.assertQueue('onem2mqe_data', {durable: false});
        ch.assertQueue('onem2mqe_cmd', {durable: false});


        for(i=0;i<n_queries;i++) {
            let cmd = {"cmd": "start_query_direct", "args": queries[i]};
            ch.sendToQueue('onem2mqe_cmd', new Buffer(JSON.stringify(cmd)));
            console.log("Sending query " + i);
        }


        x = 0;
        // SOME PROBLEM: Reading data from CSV makes input tput really slow, creating data by hand make it faster
        setTimeout(function() {
            var interval = setInterval(function () {
            
                // READING DATA FROM CSV
                
                var stream = fs.createReadStream(dataset_location);
                var csv = require('fast-csv');
                var csvStream = csv({"headers": true})
                var input_vehicle_count = [];
                var input_source = []
                
                
                csvStream.on("data", function(data){
                    input_vehicle_count.push(parseInt(data["vehicleCount"]));
                    input_source.push(data["source"]);
                });
                
                csvStream.on("end", function(data) {
                    console.log("Finished Reading Data from CSV");
                    n = input_source.length;
                    for(i=0;i<n;i++) {
                        ch.sendToQueue('onem2mqe_data', new Buffer(JSON.stringify({
                                       "timestamp": (new Date()).getTime(),
                                       "type": 0,
                                       "data": {"vehicleCount": input_vehicle_count[i]},
                                       "source": input_source[i]
                        })));
                        console.log("Sent data " + i.toString());
                    }
                    x++;
                    
                    console.log("Test #" + x.toString() + " finished")
                    if(x >= n_test) {
                        clearInterval(interval);
                    }
                });
                stream.pipe(csvStream);
                
                
                // OR, MAKE UP DATA
                /*
                n = 100000;
                l = ds_uris.length;
                for(i=0;i<n;i++) {
                    ch.sendToQueue('onem2mqe_data', new Buffer(JSON.stringify({
                                   "timestamp": (new Date()).getTime(),
                                   "type": 0,
                                   "data": {"vehicleCount": 50},
                                   "source": ds_uris[i % l]
                    })));
                    console.log("Sent data " + i.toString());
                }
                */
                
                
                
                
                
                
            }, 5000);
        },5000)


     });
});
