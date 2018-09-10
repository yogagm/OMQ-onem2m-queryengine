package onem2m.queryengine.localprocessorng;

import onem2m.queryengine.common.CSE;
import onem2m.queryengine.common.QE;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.FileReader;
import java.util.logging.Logger;

public class Global {
    public static CSE cse;
    public static QE mainQe;
    public static QE thisQe;

    private final static Logger LOGGER = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);

    public static void initializeConfig() {
        JSONParser parser = new JSONParser();
        try {
            JSONObject obj = (JSONObject) parser.parse(new FileReader("config_edge.json"));

            // CSE
            cse = new CSE((String) obj.get("cse_address"), (String) obj.get("cse_root_name"));

            // This QE
            thisQe = new QE((String) obj.get("cse_root_name"), (String) obj.get("qe_address"));
            thisQe.registerQEToCSE(cse);

            // Main QE
            mainQe = new QE((String) obj.get("main_cse_root_name"), (String) obj.get("main_qe_address"));


        } catch(Exception e) {
            e.printStackTrace();
        }
    }

}
