package onem2m.queryengine.localprocessorng;

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

public class LocalDataConsumer extends DefaultConsumer {
    JSONParser parser;
    private final static Logger LOGGER = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);


    public LocalDataConsumer(Channel channel) {
        super(channel);
        parser = new JSONParser();
        LOGGER.info("Ready to receive data");
    }

    @Override
    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws UnsupportedEncodingException {
        String message = new String(body, "UTF-8");
        //LOGGER.info("Received data: " + message);
        try {
            JSONObject input = (JSONObject) parser.parse(message);
            long type = (Long) input.get("type");
            if(type == 0) {
                String source = (String) input.get("source");
                long timestamp = (long) input.get("timestamp");
                Instant timestampX = Instant.ofEpochMilli(timestamp);
                Long duration = Duration.between(timestampX, Instant.now()).toMillis();
                //LOGGER.info("Duration: " + duration);
                input.put("id",  "container:" + source);
                input.put("duration", duration);
                LocalOpThreadManager.dataSourceOpQueue.put(input);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}

