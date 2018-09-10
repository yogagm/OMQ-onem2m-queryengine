package onem2m.queryengine.processorng;

import onem2m.queryengine.common.CSE;
import onem2m.queryengine.common.QE;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.FileReader;
import java.util.HashMap;
import java.util.logging.Logger;

public class Global {

    public static CSE cse;

    public static QE mainQe;
    public static HashMap<String,QE> localQes = new HashMap<>();

    public static Integer maxLocalOp = 10;
    public static Integer maxLocalQuery = 5;

    public static JSONObject metadataMapping;

    private final static Logger LOGGER = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);

    public static void initializeConfig() {
        JSONParser parser = new JSONParser();
        try {
            JSONObject obj = (JSONObject) parser.parse(new FileReader("config.json"));

            // CSE
            Global.cse = new CSE((String) obj.get("cse_address"), (String) obj.get("cse_root_name"));
            Global.cse.updateRemoteCSE();

            // Main QE
            mainQe = new QE((String) obj.get("cse_root_name"), (String) obj.get("qe_address"));
            mainQe.registerQEToCSE(Global.cse);

            // Remote QE ((구) Local QE)
            JSONArray localQEs = (JSONArray) obj.get("local_qes");
            for(Object localQE: localQEs) {
                String localQEName = (String) localQE;
                QE localQEObj = new QE(localQEName);
                if(localQEObj.getQeAddressFromCSE(Global.cse)) {
                    Global.localQes.put(localQEName, localQEObj);
                }

            }

            Benchmark.nQuery = (int) obj.get("number_of_queries_for_benchmark_purposes");
            Benchmark.nTest = (int) obj.get("number_of_tests_for_benchmark_purposes");
            return;
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    // TODO: Implement this function
    public static void initializeMetadataMapping() {
        Global.metadataMapping = Global.cse.gatherAllMetadataMapping();
        LOGGER.info("Initialized all metadata mapping");
    }
}
