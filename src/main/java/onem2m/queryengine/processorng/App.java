package onem2m.queryengine.processorng;


import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import onem2m.queryengine.processorng.preprocessor.PreProcessorThread;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.logging.Handler;
import java.util.logging.LogManager;
import java.util.logging.Logger;

public class App {
    public static void main( String[] args ) throws InterruptedException, IOException, TimeoutException {
        try {

            // Start all thread processing
            OpThreadManager.initQueues();
            OpThreadManager.startAllThreads();

            // Init config
            //Global.initializeConfig();
            //Global.initializeMetadataMapping();

            // Initialize CSE

            // Start query preprocessor
            CommandConsumer.startAllThreads();
            PreProcessorThread.startAllThreads();

            // Set Logger
            Logger log = LogManager.getLogManager().getLogger("");
            for (Handler h : log.getHandlers()) {

            }

            // AMQP: Data consumer
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("localhost");

            int threadN = 10;
            ExecutorService es = Executors.newFixedThreadPool(threadN);
            Connection connection = factory.newConnection(es);

            for(int i=0;i<threadN;i++) {
                Channel channel = connection.createChannel();

                channel.queueDeclare("onem2mqe_data", false, false, false, null);
                Consumer dataConsumer = new DataConsumer(channel);
                channel.basicConsume("onem2mqe_data", true, dataConsumer);
            }

            Channel channel = connection.createChannel();
            channel.queueDeclare("onem2mqe_cmd", false, false, false, null);
            Consumer newCommandConsumer = new NewCommandConsumer(channel);
            channel.basicConsume("onem2mqe_cmd", true, newCommandConsumer);


            Monitor monitor = new Monitor();
            monitor.start();
        } catch(Exception e) {
            e.printStackTrace();
        }

    }


}
