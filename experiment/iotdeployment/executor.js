const nj = require('numjs');
const spawn = require('child_process').spawn;
const async = require('async');
const readline = require('readline');
const fs = require('fs');

// Configuration
const this_ip = "192.168.0.100";
const hosts = ["192.168.0.101", "192.168.0.102", "192.168.0.103", "192.168.0.104", "127.0.0.1"]
const host_nickname = ["node1", "node2", "node3", "node4", "in"]
const queries = ["q1", "q2", "q3", "q4", "q5"]
const queries_nickname = ["q_{21}", "q_{22}", "q_{23}", "q_{24}", "q_{25}"]
const mode = process.argv[2];
const mode_nickname = {"qe": "with-qe", "cse": "without-qe"}
const qn = 5;
const iqt = 10000;



var query_lists = [];
for(i in queries) {
    for(let j=0;j<qn;j++) {
        let nickname = "+" + queries_nickname[i] + "(" + (j+1).toString() + ")";
        query_lists.push({"file_name": queries[i] + "_" + mode + ".js" , "idx": j, "nickname": nickname});
    }
}



// Monitor
var delay_monitor = {};
var bw_monitor = {};
var cpu_monitor = {};
var mem_monitor = {};
var port = 20000;
const clear_monitor = function() {
    for(x in hosts) {
        bw_monitor[hosts[x]] = [];
        cpu_monitor[hosts[x]] = [];
        mem_monitor[hosts[x]] = [];
    }
}

// Logging
var delay_log = fs.createWriteStream('plot/delay_log_' + mode + '.txt');
var bw_log = fs.createWriteStream('plot/bw_log_' + mode + '.txt');
var cpu_log = fs.createWriteStream('plot/cpu_log_' + mode + '.txt');
var mem_log = fs.createWriteStream('plot/mem_log_' + mode + '.txt');
var column_string = "\"index\"\t";
for(i in host_nickname) {
    column_string += "\"" + host_nickname[i] + " " + mode_nickname[mode] + "\"\t";
}
column_string += "\"mean\"\n";

bw_log.write(column_string);
cpu_log.write(column_string);
mem_log.write(column_string);
delay_log.write("\"index\"\t\"" + mode_nickname[mode] + "\"\n");

// Benchmarking Start
clear_monitor();
async.series([
    // Start bandwidth monitoring for each node
    function(callback) {
        async.each(hosts, function(host, callback) {
            if(host == "127.0.0.1") {
                ps = spawn('python', ['./netmon_in.py']);
                ps.stderr.pipe(process.stderr);
            } else {
                ps = spawn('ssh', ['pi@' + host, "python netmon.py"]);
            }
            
            let stdout_readline = readline.createInterface({
                input: ps.stdout
            });
            
            stdout_readline.on('line', function(line) {
                let input = JSON.parse(line);
                console.log(host, line);
                bw_monitor[host].push(input["bw"]);
                cpu_monitor[host].push(input["cpu"]);
                mem_monitor[host].push(input["mem"]);
            });
            callback(null);
        }, function(err) {
            clear_monitor();
            callback(err);
        });
    },
    
    // Run each query
    function(callback) {
        async.eachSeries(query_lists, function(query, callback) {
            let qname = query["file_name"] + "-" + query["idx"].toString();
            let nickname = query["nickname"];
            delay_monitor[nickname] = [];
            console.log("Processing " + nickname);
            let ps = spawn("node", [query["file_name"], this_ip + ":" + port.toString()]);
            //ps.stderr.pipe(process.stderr);
            let stdout_readline = readline.createInterface({
                input: ps.stdout
            });
            stdout_readline.on('line', function(line) {
                //console.log(line);
                let input = JSON.parse(line);
                delay_monitor[nickname].push(input["delay"]);
            });
            port++;
            setTimeout(function() {
                bw_log.write("\"" + nickname + "\"\t");
                cpu_log.write("\"" + nickname + "\"\t");
                mem_log.write("\"" + nickname + "\"\t");
                let mean_mean_bw = [];
                let mean_mean_mem = [];
                let mean_mean_cpu = [];
                for(let x in hosts) {
                    let mean_bw = nj.mean(bw_monitor[hosts[x]]);
                    bw_log.write(mean_bw + "\t");
                    let mean_mem = nj.mean(mem_monitor[hosts[x]]);
                    mem_log.write(mean_mem + "\t");
                    let mean_cpu = nj.mean(cpu_monitor[hosts[x]]);
                    cpu_log.write(mean_cpu + "\t");
                    
                    if(hosts[x] != "127.0.0.1") {
                        mean_mean_bw.push(mean_bw);
                        mean_mean_mem.push(mean_mem);
                        mean_mean_cpu.push(mean_cpu);
                    }
                }
                
                bw_log.write(nj.mean(mean_mean_bw).toString() + "\n");
                cpu_log.write(nj.mean(mean_mean_cpu).toString() + "\n");
                mem_log.write(nj.mean(mean_mean_mem).toString() + "\n");
                clear_monitor();
                console.log("Processed " + nickname);
                callback(null);
            }, iqt);
        }, function(err) {
            callback(err);
        });
    },
    
    // Run final measurement
    function(callback) {
        async.each(query_lists, function(query, callback) {
            let qname = query["file_name"] + "-" + query["idx"].toString();
            let nickname = query["nickname"]
            console.log("Measuring query delay " + nickname);
            delay_monitor[nickname] = [];
            
            setTimeout(function() {
                console.log("Measured query delay " + nickname);
                let mean_delay = nj.mean(delay_monitor[nickname]);
                delay_log.write("\"" + nickname + "\"\t" + mean_delay + "\n");
                callback(null);
            }, iqt);
        }, function(err) {
            callback(err);
        });
    }
]);


