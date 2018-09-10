package onem2m.queryengine.processorng;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import onem2m.queryengine.common.queryoperators.Output;
import onem2m.queryengine.common.threads.OpThread;
import org.json.simple.JSONObject;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

public class OutputOpThread extends Thread implements OpThread {
    private HashMap<String,Output> inputQueues = new HashMap<>();
    private LinkedBlockingQueue<JSONObject> inputQueue;
    ReentrantLock lock = new ReentrantLock();
    private Channel channel;
    private final static Logger LOGGER = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);

    public OutputOpThread(LinkedBlockingQueue<JSONObject> inputQueue) {
        this.inputQueue = inputQueue;
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = null;
        try {
            connection = factory.newConnection();
            channel = connection.createChannel();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public void addInput(String inputId, Output op) {
        lock.lock();
        try {
            String queryId = op.getQueryId();
            channel.queueDeclare("onem2mqe_" + queryId, false, false, false, null);
            inputQueues.put(inputId, op);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
    }

    public void run() {
        while(true) {
            try {
                JSONObject data = inputQueue.take();
                Instant startTime = Instant.now();
                String id = (String) data.get("id");
                Output outputOp = inputQueues.get(id);
                String queryId = outputOp.getQueryId();
                JSONObject input = (JSONObject) data.get("data");
                JSONObject output = outputOp.renameColumns(input);
                long timestamp = Instant.now().toEpochMilli();
                Long duration = Duration.between(startTime, Instant.now()).toMillis();

                Long prevDuration = (Long) data.get("duration");
                data.put("timestamp", timestamp);



                //System.out.println(output);
                //channel.basicPublish("", "onem2mqe_" + queryId, null, data.toJSONString().getBytes());
                //LOGGER.info("Duration: " + prevDuration);

                Benchmark.addProcessingTime(prevDuration + duration);
                Benchmark.increamentTput();

                Long durationFinal = Duration.between(startTime, Instant.now()).toMillis();
                Benchmark.addOutputTime(durationFinal);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


}
