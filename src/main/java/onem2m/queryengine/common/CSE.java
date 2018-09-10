package onem2m.queryengine.common;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import org.apache.http.client.utils.URIBuilder;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.logging.Logger;

public class CSE {
    private JSONParser parser = new JSONParser();
    private String cseAddress = null;
    private String rootCSE = null;
    public  ArrayList<CSE> remoteCSEs;
    private static HashMap<String,CSE> cses = new HashMap<>();
    private final static Logger LOGGER = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);

    public CSE(String cseAddress, String rootCSE) {
        this.cseAddress = cseAddress;
        this.rootCSE = rootCSE;
    }

    public String getRootCSE() {
        return this.rootCSE;
    }

    public static JSONObject createHeaders(boolean sending, int resourceType) {
        // By default, assume Mobius
        JSONObject result = new JSONObject();
        if(sending == true) {
            result.put("X-M2M-Origin", "SOrigin");
            result.put("X-M2M-RI", "12345");
            result.put("Accept", "application/json");
            result.put("Content-Type", "application/json;ty=" + resourceType);
        } else {
            result.put("X-M2M-Origin", "SOrigin");
            result.put("X-M2M-RI", "12345");
            result.put("Accept", "application/json");
        }

        return result;


    }

    public JSONObject get(String path, JSONObject body, boolean viaRi) {
        return this.getRemote(this.rootCSE, path, body, viaRi);
    }

    public JSONObject post(String path, int ty, JSONObject body) {
        return this.postRemote(this.rootCSE, ty, path, body);
    }

    public JSONObject getRemote(String rootCSE, String path, JSONObject body, boolean viaRi) {
        try {
            URIBuilder address = new URIBuilder(cseAddress);

            if(viaRi) {
                address.setPath(path);
            } else {
                address.setPath(rootCSE);
                address.setPath(address.getPath() + path);
            }

            HttpResponse<String> response = Unirest.get(address.toString())
                    .headers(createHeaders(false, 0))
                    .queryString(body)
                    .asString();

            if (response.getStatus() == 200 || response.getStatus() == 201) {
                return (JSONObject) parser.parse(response.getBody());
            }
        } catch(Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    public JSONObject postRemote(String rootCSE, int ty, String path, JSONObject body)  {
        try {
            URIBuilder address = new URIBuilder(cseAddress);
            address.setPath(rootCSE);
            address.setPath(address.getPath() + path);
        
            HttpResponse<String> response = Unirest.post(address.toString())
                    .headers(createHeaders(true, ty))
                    .body(body.toJSONString())
                    .asString();

            //LOGGER.info("POST: " + address.toString() + ": " + response.getBody());
            if (response.getStatus() == 200 || response.getStatus() == 201) {
                return (JSONObject) parser.parse(response.getBody());
            } else {

                return putRemote(rootCSE, path, ty, body);
            }
        } catch(Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    private JSONObject putRemote(String rootCSE, String path, int ty, JSONObject body) {
        try {
            String firstKey = (String) body.keySet().toArray()[0];
            JSONObject bodyBody = (JSONObject) body.get(firstKey);

            URIBuilder address = new URIBuilder(cseAddress);
            address.setPath(rootCSE);
            address.setPath(address.getPath() + path);
            address.setPath(address.getPath() + "/" + bodyBody.get("rn"));

            // Remove some NP column
            bodyBody.remove("rn");
            bodyBody.remove("api");

            HttpResponse<String> response = Unirest.put(address.toString())
                    .headers(createHeaders(true, ty))
                    .body(body.toJSONString())
                    .asString();

            //LOGGER.info("PUT: " + address.toString() + ": " + response.getBody());
            if (response.getStatus() == 200 || response.getStatus() == 201) {
                return (JSONObject) parser.parse(response.getBody());
            }
        } catch(Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    public JSONObject gatherResourceTree() {
        return gatherRemoteResourceTree(this.rootCSE);
    }

    public JSONObject gatherRemoteResourceTree(String rootCSE) {
        try {
            JSONObject input = new JSONObject();
            input.put("rcn", 5);
            JSONObject result = getRemote(rootCSE, "", input, false);
            if (result.containsKey("m2m:rsp")) {
                return (JSONObject) result.get("m2m:rsp");

            } else {
                return result;
            }

        } catch(Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    public void updateRemoteCSE() {
        ArrayList<CSE> result = new ArrayList<>();
        JSONObject resourceTree = this.gatherResourceTree();
        JSONArray remoteCSEEntries = (JSONArray) resourceTree.get("m2m:csr");

        for(Object remoteCSEEntry: remoteCSEEntries) {
            JSONObject remoteCSEEntryX = (JSONObject) remoteCSEEntry;
            String name = (String) remoteCSEEntryX.get("cb");
            JSONArray uris = (JSONArray) remoteCSEEntryX.get("poa");
            String uri = (String) uris.get(0);

            CSE newCSE = new CSE(uri, name);
            result.add(newCSE);
        }

        LOGGER.info("Initialized remote CSE connections");

        this.remoteCSEs = result;
    }

    // Metadata Mapping Related
    public JSONObject gatherMetadataMapping() {
        return gatherRemoteMetadataMapping(this.rootCSE);
    }

    public JSONObject gatherRemoteMetadataMapping(String rootCSE) {
        JSONObject aesMetadata = new JSONObject();
        JSONObject containersMetadata = new JSONObject();
        JSONObject metadataMapping = new JSONObject();
        JSONObject resourceTree = gatherRemoteResourceTree(rootCSE);
        JSONArray cinEntries = (JSONArray) resourceTree.get("m2m:cin");
        try {
            if(cinEntries == null) {
                return metadataMapping;
            }

            for (Object cin : cinEntries) {
                JSONObject cinX = (JSONObject) cin;
                String[] pi = ((String) cinX.get("pi")).split("/");
                if (pi[pi.length - 1].equals("metadata")) {
                    String resName = pi[2];
                    JSONObject con = (JSONObject) parser.parse((String) cinX.get("con"));
                    aesMetadata.put(resName, con);
                } else if (pi[pi.length - 1].startsWith("xx")) {
                    String removedXX = pi[3].substring(2, pi[3].length());
                    String resName = pi[2] + "/" + removedXX;
                    JSONObject con = (JSONObject) parser.parse((String) cinX.get("con"));
                    containersMetadata.put(resName, con);
                }
            }

            // Post processing
            for (Object container : containersMetadata.keySet()) {
                String containerX = (String) container;
                String key = "/" + rootCSE + "/" + containerX;
                String aeLocation = containerX.split("/")[0];
                JSONObject containerMetadata = (JSONObject) containersMetadata.get(container);
                JSONObject aeMetadata = (JSONObject) aesMetadata.get(aeLocation);
                containerMetadata.putAll(aeMetadata);
                metadataMapping.put(key, containerMetadata);
            }
        } catch(Exception e) {
            e.printStackTrace();
        }
        LOGGER.info("Initialized metadata mapping for CSE: " + rootCSE);
        return metadataMapping;
    }

    public JSONObject gatherAllMetadataMapping() {
        JSONObject results = new JSONObject();

        // Get root's metadata mapping
        results.putAll(gatherMetadataMapping());

        for(CSE remoteCSE: remoteCSEs) {
            results.putAll(gatherRemoteMetadataMapping(remoteCSE.getRootCSE()));
        }

        return results;
    }


}
