package onem2m.queryengine.processorng;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.UnsupportedEncodingException;
import java.time.Duration;
import java.time.Instant;
import java.util.logging.Logger;

public class DataConsumer extends DefaultConsumer {
    JSONParser parser;
    public static Boolean timerStarted = false;
    boolean isExternalQuery = false;
    private final static Logger LOGGER = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);


    public DataConsumer(Channel channel) {
        super(channel);
        parser = new JSONParser();
        LOGGER.info("Ready to receive data");
    }

    @Override
    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws UnsupportedEncodingException {
        //LOGGER.info("New data received");
        Instant startTime = Instant.now();


        if(!timerStarted) {
            Benchmark.startTimer();
            timerStarted = true;
        }

        String message = new String(body, "UTF-8");
        try {
            JSONObject input = (JSONObject) parser.parse(message);
            //JSONObject input = JsonIterator.deserialize(message, JSONObject.class);
            //LOGGER.info("Received data");
            long type = (Long) input.get("type");
            if(type == 0) {
                String source = (String) input.get("source");
                long timestamp = (long) input.get("timestamp");
                Instant timestampX = Instant.ofEpochMilli(timestamp);
                Long duration = Duration.between(timestampX, Instant.now()).toMillis();
                input.put("id",  "container:" + source);
                input.put("duration", new Long(0));

                OpThreadManager.dataSourceOpQueue.put(input);
                Benchmark.increamentInputTput();
            } else if(type == 1) {
                //LOGGER.info("Received data for external query: " + input.toJSONString());
                String source = (String) input.get("source");
                Long prevDuration = (long) input.get("duration");
                long timestamp = (long) input.get("timestamp");
                Instant timestampX = Instant.ofEpochMilli(timestamp);
                Long duration = Duration.between(timestampX, Instant.now()).toMillis();
                //LOGGER.info("Duration: " + duration);
                JSONObject data = (JSONObject) input.get("data");
                for(Object key: data.keySet()) {
                    String columnName = (String) key;
                    JSONObject inputX = new JSONObject();
                    inputX.put("id", "extquery:" + source + ":" + columnName);
                    inputX.put("data", data.get(key));
                    inputX.put("duration", prevDuration + duration);
                    OpThreadManager.dataSourceOpQueue.put(inputX);
                    Benchmark.increamentInputTput();
                }
            }

            // Benchmark code
            Long durationFinal = Duration.between(startTime, Instant.now()).toMillis();
            Benchmark.addDataConsumerTime(durationFinal);
        } catch (Exception e) {
            e.printStackTrace();
        }
        //LOGGER.info("New data processed");

    }

}
