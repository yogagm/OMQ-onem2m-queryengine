package onem2m.queryengine.localprocessorng;


import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class App {
    // NOTE: In the meantime, always start Local Processor NG on each edge first, before starting it in the infrastructure
    public static void main( String[] args ) throws IOException, TimeoutException {
        Global.initializeConfig();

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        // Command Consumer: Thread
        LocalCommandConsumer.startAllThreads();

        // Data consumer: AMQP
        channel.queueDeclare("onem2mqe_local_data", false, false, false, null);
        Consumer dataConsumer = new LocalDataConsumer(channel);
        channel.basicConsume("onem2mqe_local_data", true, dataConsumer);

        // Command consumer: AMQP
        channel.queueDeclare("onem2mqe_local_cmd", false, false, false, null);
        Consumer localNewCommandConsumer = new LocalNewCommandConsumer(channel);
        channel.basicConsume("onem2mqe_local_cmd", true, localNewCommandConsumer);

        // Start processing
        LocalOpThreadManager.initQueues();
        LocalOpThreadManager.startAllThreads();
    }
}
