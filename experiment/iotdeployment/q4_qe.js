const net = require('net');
const id = process.argv[2]
var query = {
    "id": id,
    "select": {
        "cpu_usage_00": {"source": {"location": "mn-0", "sensor_type": "cpu_usage"}, 
                         "1-aggregate": {"function": "mean", "window_size": 60, "output_rate": 5}},
        "cpu_usage_01": {"source": {"location": "mn-1", "sensor_type": "cpu_usage"}, 
                         "1-aggregate": {"function": "mean", "window_size": 60, "output_rate": 5}},
        "cpu_usage_02": {"source": {"location": "mn-2", "sensor_type": "cpu_usage"}, 
                         "1-aggregate": {"function": "mean", "window_size": 60, "output_rate": 5}},
        "cpu_usage_03": {"source": {"location": "mn-3", "sensor_type": "cpu_usage"}, 
                         "1-aggregate": {"function": "mean", "window_size": 60, "output_rate": 5}},   
    },
    "1-transform": {
        "function": "mean",
        "output_column": "mean_cpu_usage"
    }
}

var client = net.createConnection({port: 9999}, function() {
    cmd = {"cmd": "start_query", "args": query};
    client.write(JSON.stringify(cmd) + "\n");
});

var amqp = require('amqplib/callback_api');
var sleep = require('sleep');
amqp.connect('amqp://192.168.0.100', function(err, conn) {
     conn.createChannel(function(err, ch) {
         ch.assertQueue('onem2mqe_' + id, {durable: false});
         ch.consume('onem2mqe_' + id, function(msg) {
            msgString =  msg.content.toString()
            input = JSON.parse(msgString);
            timestamp = input["timestamp"]
            now = (new Date()).getTime();
            delay = input["duration"] + (now - timestamp)
            output = {"delay": delay,"port": id}
            //console.log(input);
            console.log(JSON.stringify(output));
         }, {noAck: true});
     });
});


