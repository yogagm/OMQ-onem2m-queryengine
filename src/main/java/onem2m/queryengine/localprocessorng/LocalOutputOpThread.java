package onem2m.queryengine.localprocessorng;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import onem2m.queryengine.common.threads.OpThread;
import org.json.simple.JSONObject;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

public class LocalOutputOpThread extends Thread implements OpThread {
    private ArrayList<String> inputIds = new ArrayList<>();
    private HashMap<String, String> prevIdMapping = new HashMap<>();  // Map prevId with inputIds
    private LinkedBlockingQueue<JSONObject> inputQueue;
    private final static Logger LOGGER = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
    private Channel channel;

    ReentrantLock lock = new ReentrantLock();

    public LocalOutputOpThread(LinkedBlockingQueue<JSONObject> inputQueue) {

        this.inputQueue = inputQueue;
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(Global.mainQe.getQeAddress());
        Connection connection = null;
        try {
            connection = factory.newConnection();
            channel = connection.createChannel();
            channel.queueDeclare("onem2mqe_data", false, false, false, null);
            LOGGER.info("Connected to main QE queue server");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public void addInput(String inputId, String prevId) {
        lock.lock();
        try {
            inputIds.add(inputId);
            prevIdMapping.put(prevId, inputId);
        } finally {
            lock.unlock();
        }
    }

    public void run() {
        while(true) {
            try {
                JSONObject toSend = new JSONObject();
                JSONObject output = new JSONObject();
                JSONObject input = inputQueue.take();
                String id = (String) input.get("id");
                JSONObject data = (JSONObject) input.get("data");
                Long duration = (long) input.get("duration");

                if(data.size() == 1) {
                    for(Object dataKey: data.keySet()) {
                        output.put(id, data.get(dataKey));
                    }
                } else {
                    // Remove before "." part of each column in data
                    JSONObject newData = new JSONObject();
                    for(Object dataKey: data.keySet()) {
                        String[] dataKeySplit = dataKey.toString().split("\\.", 2);
                        String newColumnName = dataKeySplit[1];
                        newData.put(newColumnName, data.get(dataKey));
                    }
                    output.put(id, newData);
                }


                toSend.put("source", Global.thisQe.getQeName());
                toSend.put("type", 1);
                toSend.put("duration", duration);
                long timestamp = Instant.now().toEpochMilli();
                toSend.put("timestamp", timestamp);
                toSend.put("data", output);
                channel.basicPublish("", "onem2mqe_data", null, toSend.toJSONString().getBytes());
                //System.out.println(toSend);
                //LOGGER.info("Sent to main QE: " + toSend.toJSONString());
                //LOGGER.info("Duration: " + duration);

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public String getInputId(String prevId) {
        return prevIdMapping.get(prevId);
    }
}
