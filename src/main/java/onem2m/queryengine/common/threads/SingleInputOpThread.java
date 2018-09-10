package onem2m.queryengine.common.threads;

import onem2m.queryengine.common.queryoperators.Operator;
import org.apache.commons.lang3.tuple.Pair;
import org.json.simple.JSONObject;

import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

public class SingleInputOpThread extends Thread implements OpThread {
    protected HashMap<String, Pair<Operator, ArrayList<Pair<String, Operator>>>> relation = new HashMap<>();
    protected HashMap<Class, LinkedBlockingQueue<JSONObject>> otherInputQueues;
    protected LinkedBlockingQueue<JSONObject> inputQueue;
    ReentrantLock lock = new ReentrantLock();
    private final static Logger LOGGER = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);

    public SingleInputOpThread(LinkedBlockingQueue<JSONObject> inputQueue, HashMap<Class, LinkedBlockingQueue<JSONObject>> otherInputQueues) {
        this.inputQueue = inputQueue;
        this.otherInputQueues = otherInputQueues;
    }

    public boolean isInputExist(String inputId) {
        return this.relation.containsKey(inputId);
    }

    public ArrayList<Pair<String, ArrayList<String>>> getOperatorConnections(Operator op) {
        ArrayList<Pair<String, ArrayList<String>>> output = new ArrayList<>();
        for(String prevId: relation.keySet()) {
            Pair<Operator, ArrayList<Pair<String, Operator>>> processingTriple = relation.get(prevId);
            Operator toCompare = processingTriple.getLeft();
            if (toCompare.equals(op)) {
                ArrayList<String> nextIds = new ArrayList<>();
                for (Pair<String, Operator> outputPair : processingTriple.getRight()) {
                    nextIds.add(outputPair.getLeft());
                }
                output.add(Pair.of(prevId, nextIds));
            }
        }

        return output;
    }

    public void addInput(String inputId) {
        lock.lock();
        try {
            //Pair<Operator, ArrayList<Pair<String, Operator>>> newArray = ;
            //relation.put(inputId, null);
        } finally {
            lock.unlock();
        }
    }

    public void addConnection(String inputId, String outputId, Operator op, Operator nextOp) {
        lock.lock();
        try {
            //ArrayList<Pair<String, Operator>> newOutput = new ArrayList<>();
            //newOutput.add(Pair.of(outputId, nextOp));
            // TODO before just adding it, check if same op exist there
            if(!relation.containsKey(inputId)) {
                ArrayList<Pair<String, Operator>> newArray = new ArrayList<>();
                relation.put(inputId, Pair.of(op, newArray));
            }

            relation.get(inputId).getRight().add(Pair.of(outputId, nextOp));
        } finally {
            lock.unlock();
        }
    }

    public Pair<Operator, ArrayList<Pair<String, Operator>>> getInputConnection(String inputId) {
        return relation.get(inputId);
    }

    public JSONObject process(Object data, Operator processingOp) throws InvocationTargetException, IllegalAccessException {
        return (JSONObject) data;
    }

    protected void benchmarkTiming(Long duration) {
        return;
    }

    public void run() {
        while(true) {
            try {
                JSONObject input = inputQueue.take();
                Instant startTime = Instant.now();
                String inputId = (String) input.get("id");
                Object data = input.get("data");
                Long prevDuration = (Long) input.get("duration");

                Pair<Operator, ArrayList<Pair<String, Operator>>> processingTriple = this.relation.get(inputId);
                if(processingTriple != null) {
                    Operator op = processingTriple.getLeft();
                    JSONObject result = process(data, op);
                    if(result != null) {
                        ArrayList<Pair<String, Operator>> outputPairs = processingTriple.getRight();
                        for(Pair<String, Operator> outputPair: outputPairs) {
                            String outputId = outputPair.getLeft();
                            //LOGGER.info("Received data from queue: " + inputId + ". Processing it into queue: " + outputId);
                            Class nextOp = outputPair.getRight().getClass();
                            JSONObject output = new JSONObject();
                            output.put("id", outputId);
                            output.put("data", result);
                            Long duration = Duration.between(startTime, Instant.now()).toMillis();
                            output.put("duration", prevDuration + duration);
                            otherInputQueues.get(nextOp).add(output);
                        }
                    }
                }

                Long durationFinal = Duration.between(startTime, Instant.now()).toMillis();
                benchmarkTiming(durationFinal);
            } catch (Exception e) {
                e.printStackTrace();
            }


        }
    }



    @Override
    public String toString() {
        String output = "";
        output += "====== " + this.getClass().getName() + " =====\n";
        for(String prevId: relation.keySet()) {
            output += " * " + prevId + ": ";
            Pair<Operator, ArrayList<Pair<String, Operator>>> processingTriple = relation.get(prevId);
            Operator toCompare = processingTriple.getLeft();
            output += "(" + toCompare + ") ";
            for(Pair<String, Operator> outputPair: processingTriple.getRight()) {
                output += outputPair.getLeft() + ", ";
            }
            output += "\n";
        }
        output += "\n";
        return output;
    }
}
