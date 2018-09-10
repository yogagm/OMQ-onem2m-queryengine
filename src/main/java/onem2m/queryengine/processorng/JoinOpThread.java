package onem2m.queryengine.processorng;

import onem2m.queryengine.common.queryoperators.Operator;
import onem2m.queryengine.common.threads.OpThread;
import org.apache.commons.lang3.tuple.Triple;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

public class JoinOpThread extends Thread implements OpThread {


    private HashMap<String, ArrayList<Triple<String, Operator, Operator>>> relation = new HashMap<>(); // Map joinId --> outputTriplets
    private HashMap<String, ArrayList<String>> secondMapping = new HashMap<>();   // Map joinId --> inputIds
    private HashMap<String, String> thirdMapping = new HashMap<>();  // Map inputId --> joinId
    private HashMap<ArrayList<String>, String> fourthMapping = new HashMap<>(); // Map inputIds --> joinId
    private HashMap<String, JSONArray> buffers = new HashMap<>();
    private HashMap<String, Long> prevDurations = new HashMap<>();
    private HashMap<Class, LinkedBlockingQueue<JSONObject>> otherInputQueues;
    private LinkedBlockingQueue<JSONObject> inputQueue;
    private final static Logger LOGGER = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
    ReentrantLock lock = new ReentrantLock();

    public JoinOpThread(LinkedBlockingQueue<JSONObject> inputQueue, HashMap<Class, LinkedBlockingQueue<JSONObject>> otherInputQueues) {
        this.inputQueue = inputQueue;
        this.otherInputQueues = otherInputQueues;
    }

    public String addInput(ArrayList<String> inputIds) {
        lock.lock();
        String joinId;
        try {
            joinId = UUID.randomUUID().toString();
            ArrayList<Triple<String, Operator, Operator>> newArray = new ArrayList<Triple<String, Operator, Operator>>();
            relation.put(joinId, newArray);
            secondMapping.put(joinId, inputIds);
            fourthMapping.put(inputIds, joinId);
            for(String inputId: inputIds) {
                thirdMapping.put(inputId, joinId);
            }

            buffers.put(joinId, new JSONArray());
            prevDurations.put(joinId, new Long(0));

        } finally {
            lock.unlock();
        }

        return joinId;
    }

    public void addConnection(ArrayList<String> inputIds, String outputId, Operator op, Operator nextOp) {
        lock.lock();
        try {
            String joinId = fourthMapping.get(inputIds);
            relation.get(joinId).add(Triple.of(outputId, op, nextOp));
        } finally {
            lock.unlock();
        }
    }

    public void addConnection(String joinId, String outputId, Operator op, Operator nextOp) {
        lock.lock();
        try {
            relation.get(joinId).add(Triple.of(outputId, op, nextOp));
        } finally {
            lock.unlock();
        }
    }

    public void run() {
        while(true) {
            try {
                JSONObject input = inputQueue.take();
                //System.out.println(inputQueue.size());

                Instant startTime = Instant.now();
                String inputId =  (String) input.get("id");
                String joinId = thirdMapping.get(inputId);
                ArrayList<String> inputIds = secondMapping.get(joinId);
                JSONObject data = (JSONObject) input.get("data");
                JSONArray buffer = this.buffers.get(joinId);

                Long prevDuration = (Long) input.get("duration");
                Long prevDurationBefore = prevDurations.get(joinId);
                prevDurations.put(joinId, prevDuration + prevDurationBefore);

                buffer.add(data);

                // [NEED TESTING] TODO: (json-data-support) The buffer size can be larger than maximum inputId size, so change the buffer mechanism to get a better detection

                if(buffer.size() == inputIds.size()) {
                    ArrayList<Triple<String, Operator, Operator>> outputTriples = relation.get(joinId);
                    for(Triple<String, Operator, Operator> outputTriple: outputTriples) {
                        Class nextOp = outputTriple.getRight().getClass();
                        String outputId = outputTriple.getLeft();
                        JSONObject output = new JSONObject();
                        JSONObject dataOutput = new JSONObject();
                        for(Object x: buffer.toArray()) {
                            JSONObject fromBuffer = (JSONObject) x;
                            dataOutput.putAll(fromBuffer);
                        }
                        output.put("id", outputId);
                        output.put("data", dataOutput);
                        Long duration = Duration.between(startTime, Instant.now()).toMillis();

                        output.put("duration", (prevDurations.get(joinId) / buffer.size()) + duration);
                        otherInputQueues.get(nextOp).put(output);
                        prevDurations.put(joinId, new Long(0));
                    }
                    buffer.clear();
                }

                Long durationFinal = Duration.between(startTime, Instant.now()).toMillis();
                Benchmark.addJoinTime(durationFinal);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }


    public HashMap<String, ArrayList<String>> getInputConnections() {
        return secondMapping;
    }

    public HashMap<String,String> getInputJoinIdMapping() {
        return thirdMapping;
    }
}
