package onem2m.queryengine.common;

import onem2m.queryengine.common.queryoperators.ContainerDataSource;
import onem2m.queryengine.processorng.Global;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.util.HashMap;
import java.util.logging.Logger;

public class QE {
    private String qeName;
    private String qeAddress;
    private final static Logger LOGGER = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);

    public QE(String qeName) {
        this.qeName = qeName;
    }

    public QE(String qeName, String qeAddress) {
        this.qeName = qeName;
        this.qeAddress = qeAddress;
    }

    public String getQeAddress() {
        return qeAddress;
    }

    public void setQeAddress(String qeAddress) {
        this.qeAddress = qeAddress;
    }

    public String getQeName() {
        return qeName;
    }

    public void setQeName(String qeName) {
        this.qeName = qeName;
    }

    // TODO: Decide the POA: Notif receiver server of an QE
    public void registerQEToCSE(CSE cse)  {
        // Only permits creation of QE AE in the main CSE
        if(qeName == cse.getRootCSE()) {
            JSONObject newAE = new JSONObject();
            newAE.put("rn", "query-engine");
            newAE.put("api", 12345);
            newAE.put("rr", true);
            JSONArray poa = new JSONArray();
            poa.add(this.qeAddress);
            newAE.put("poa", poa);

            JSONObject output = new JSONObject();
            output.put("m2m:ae", newAE);
            cse.post("", 2, output);
            LOGGER.info("Registered the QE to " + cse.getRootCSE());
        }

    }

    public boolean getQeAddressFromCSE(CSE cse) {
        JSONObject resourceTree = cse.gatherRemoteResourceTree(this.qeName);
        JSONArray remoteCSEEntries = (JSONArray) resourceTree.get("m2m:ae");

        for(Object remoteCSEEntry: remoteCSEEntries) {
            JSONObject remoteCSEEntryX = (JSONObject) remoteCSEEntry;
            String resourceName = (String) remoteCSEEntryX.get("rn");
            JSONArray uris = (JSONArray) remoteCSEEntryX.get("poa");
            if(resourceName.contains("query-engine")) {
                if(uris.size() == 1) {
                    this.qeAddress =  (String) uris.get(0);
                    return true;
                }
            }

        }
        return false;
    }

    public HashMap<String, String> sendSubQuery(SubQuery sq) {
        String localQeAddress = this.qeAddress;
        try {
            JSONParser parser = new JSONParser();
            Socket client = new Socket(localQeAddress, 9999);
            BufferedReader inputStream = new BufferedReader(new InputStreamReader(client.getInputStream()));
            BufferedWriter outputStream = new BufferedWriter(new OutputStreamWriter(client.getOutputStream()));
            JSONObject cmd = new JSONObject();
            cmd.put("cmd", "start_local_query");
            cmd.put("args", sq.toJSON());
            outputStream.write(cmd.toJSONString());
            outputStream.newLine();
            outputStream.flush();
            System.out.println(cmd);

            // 1b. Retrieve callback answer
            String mappingString = inputStream.readLine();
            JSONObject mapping = (JSONObject) parser.parse(mappingString);

            return mapping;


        } catch(Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    public static QE getQeOfDataSource(ContainerDataSource dsX) {
        String[] containerSplit = dsX.getContainer().split("/");
        if(containerSplit.length > 1) {
            String hostName = containerSplit[1];
            if(Global.localQes.containsKey(hostName)) {
                return Global.localQes.get(hostName);
            }
        }

        return Global.mainQe;
    }


}
