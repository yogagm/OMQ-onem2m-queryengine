package onem2m.queryengine.processorng.preprocessor;

import onem2m.queryengine.common.QE;
import onem2m.queryengine.common.Query;
import onem2m.queryengine.common.SubQuery;
import onem2m.queryengine.processorng.Global;
import org.json.simple.JSONObject;

import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class PreProcessorThread extends Thread {
    private static LinkedBlockingQueue<JSONObject> inputQueue = new LinkedBlockingQueue<>();
    private static PreProcessorThread t;

    public static void startAllThreads() {
        t = new PreProcessorThread();
        t.start();
    }

    public static void newQuery(JSONObject newQuery) {
        inputQueue.add(newQuery);
    }


    // TODO: Test with 1 IN - 4 MN
    @Override
    public void run() {
        while(true) {
            JSONObject newQuery = null;
            try {
                newQuery = inputQueue.take();

                // Data Source Resolver
                JSONObject resolvedQuery = DataSourceResolver.resolveQuery(newQuery);
                Query newQueryObj = new Query(resolvedQuery);

                // Split Query
                HashMap<QE, SubQuery> subQueries = QuerySplitter.splitQuery(newQueryObj);
                SubQuery internalQuery = subQueries.get(Global.mainQe);

                // Forward Query
                for (QE qe : subQueries.keySet()) {
                    if (!(qe == Global.mainQe)) {
                        SubQuery externalQuery = subQueries.get(qe);

                        // Change internal query external query data source to reflect its external query
                        HashMap<String, String> mapping = Global.localQes.get(qe.getQeName()).sendSubQuery(externalQuery);
                        internalQuery.updateExternalQueryMapping(qe.getQeName(), mapping);
                    }
                }

                // Provision Internal Query
                internalQuery.provisionQueryNg();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


}
