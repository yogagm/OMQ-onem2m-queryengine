package onem2m.queryengine.localprocessorng;

import onem2m.queryengine.common.Query;

import javax.jws.WebService;
import java.util.HashMap;

@WebService(endpointInterface = "onem2m.queryengine.localprocessorng.LocalProcessorCommands")
public class LocalProcessorCommandsImpl implements  LocalProcessorCommands {
    @Override
    public HashMap<String, String> addSubQuery(Query q) {
        HashMap<String, String> results = new HashMap<>();
        results.put("nama", q.getId());
        System.out.println(results);
        return results;
    }
}
