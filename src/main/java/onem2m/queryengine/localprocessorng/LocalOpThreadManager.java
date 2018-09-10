package onem2m.queryengine.localprocessorng;

import onem2m.queryengine.common.queryoperators.*;
import onem2m.queryengine.common.threads.*;
import org.json.simple.JSONObject;

import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class LocalOpThreadManager {
    private static HashMap<Class, LinkedBlockingQueue<JSONObject>> otherInputQueues = new HashMap<>();

    public static LinkedBlockingQueue<JSONObject> dataSourceOpQueue = new LinkedBlockingQueue<>();
    public static LinkedBlockingQueue<JSONObject> aggrOpQueue = new LinkedBlockingQueue<>();
    public static LinkedBlockingQueue<JSONObject> tranOpQueue = new LinkedBlockingQueue<>();
    public static LinkedBlockingQueue<JSONObject> filterOpQueue = new LinkedBlockingQueue<>();
    public static LinkedBlockingQueue<JSONObject> outputOpQueue = new LinkedBlockingQueue<>();

    public static DataSourceOpThread dataSourceOpThread = new DataSourceOpThread(dataSourceOpQueue, otherInputQueues);
    public static AggregationOpThread aggrOpThread = new AggregationOpThread(aggrOpQueue, otherInputQueues);
    public static TransformationOpThread tranOpThread = new TransformationOpThread(tranOpQueue, otherInputQueues);
    public static FilterOpThread filterOpThread = new FilterOpThread(filterOpQueue, otherInputQueues);
    public static LocalOutputOpThread outputOpThread = new LocalOutputOpThread(outputOpQueue);

    public static OpThread getOpThread(Class c) {
        if(c == Aggregation.class) {
            return aggrOpThread;
        } else if(c == Transformation.class) {
            return tranOpThread;
        } else if (c == Filter.class) {
            return filterOpThread;
        }
        return null;
    }

    public static void initQueues() {
        otherInputQueues.put(DataSource.class, dataSourceOpQueue);
        otherInputQueues.put(Aggregation.class, aggrOpQueue);
        otherInputQueues.put(Transformation.class, tranOpQueue);
        otherInputQueues.put(Filter.class, filterOpQueue);
        otherInputQueues.put(Output.class, outputOpQueue);
    }

    public static void startAllThreads() {
        dataSourceOpThread.start();
        aggrOpThread.start();
        tranOpThread.start();
        filterOpThread.start();
        outputOpThread.start();
    }
}
