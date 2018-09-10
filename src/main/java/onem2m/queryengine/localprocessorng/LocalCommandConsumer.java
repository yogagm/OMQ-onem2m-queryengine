package onem2m.queryengine.localprocessorng;

import onem2m.queryengine.common.SubQuery;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.logging.Logger;

public class LocalCommandConsumer extends Thread {
    private ServerSocket serverSocket;
    private JSONParser parser;
    private final static Logger LOGGER = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
    private static LocalCommandConsumer t;

    public LocalCommandConsumer() throws IOException {
        serverSocket = new ServerSocket(9999);
        parser = new JSONParser();
        LOGGER.info("Ready to receive command");
    }

    public void run() {
        while(true) {
            Socket server = null;
            try {
                server = serverSocket.accept();
                LOGGER.info("Received a new command connection");
                BufferedReader inputStream = new BufferedReader(new InputStreamReader(server.getInputStream()));
                BufferedWriter outputStream = new BufferedWriter(new OutputStreamWriter(server.getOutputStream()));
                String input = inputStream.readLine();
                JSONObject inputJson = (JSONObject) parser.parse(input);
                String command = (String) inputJson.get("cmd");
                JSONObject arg = (JSONObject) inputJson.get("args");

                switch(command) {
                    case "start_local_query":
                        SubQuery query = new SubQuery(arg);
                        HashMap<String, String> mapping = query.provisionLocalQueryNg();
                        JSONObject mappingJSON = new JSONObject(mapping);
                        outputStream.write(mappingJSON.toJSONString());
                        outputStream.newLine();
                        outputStream.flush();
                        break;
                }

                LOGGER.info("Command processing finish");

                server.close();



            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    public static void startAllThreads() throws IOException {
        t = new LocalCommandConsumer();
        t.start();
    }
}
