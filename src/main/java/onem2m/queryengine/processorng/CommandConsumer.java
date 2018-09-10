package onem2m.queryengine.processorng;

import onem2m.queryengine.common.SubQuery;
import onem2m.queryengine.processorng.preprocessor.PreProcessorThread;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.logging.Logger;


public class CommandConsumer extends Thread {
    private ServerSocket serverSocket;
    private JSONParser parser;
    private final static Logger LOGGER = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
    private static CommandConsumer t;

    public CommandConsumer() throws IOException {
        serverSocket = new ServerSocket(9999);
        parser = new JSONParser();

    }

    public void run() {
        LOGGER.info("Ready to receive command");
        while(true) {
            Socket server = null;
            try {
                server = serverSocket.accept();
                LOGGER.info("Received a new command connection");
                BufferedReader inputStream = new BufferedReader(new InputStreamReader(server.getInputStream()));
                BufferedWriter outputStream = new BufferedWriter(new OutputStreamWriter(server.getOutputStream()));
                String input = inputStream.readLine();
                //System.out.println(input);
                JSONObject inputJson = (JSONObject) parser.parse(input);
                String command = (String) inputJson.get("cmd");
                JSONObject arg = (JSONObject) inputJson.get("args");

                switch(command) {
                    case "start_query":
                        PreProcessorThread.newQuery(arg);
                        break;
                    case "start_query_direct":
                        SubQuery query = new SubQuery(arg);
                        query.adjustArgsFieldNaming();
                        //System.out.println(query.toJSON());
                        query.provisionQueryNg();
                        outputStream.newLine();
                        outputStream.flush();
                        break;
                }

                LOGGER.info("Finish processing command");



            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void startAllThreads() throws IOException {
        t = new CommandConsumer();
        t.start();
    }
}
