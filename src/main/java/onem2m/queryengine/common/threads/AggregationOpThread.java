package onem2m.queryengine.common.threads;


import onem2m.queryengine.common.queryoperators.Aggregation;
import onem2m.queryengine.common.queryoperators.Operator;
import onem2m.queryengine.processorng.Benchmark;
import org.apache.commons.lang3.tuple.Pair;
import org.json.simple.JSONObject;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.*;

public class AggregationOpThread extends SingleInputOpThread {
    private HashMap<String, Boolean> outputted = new HashMap<>();
    private ScheduledExecutorService execService = Executors.newScheduledThreadPool(1);
    private ExecutorService processExecService = Executors.newFixedThreadPool(4);
    public AggregationOpThread(LinkedBlockingQueue<JSONObject> inputQueue, HashMap<Class, LinkedBlockingQueue<JSONObject>> otherInputQueues) {
        super(inputQueue, otherInputQueues);
    }


    @Override
    public void run() {
        while(true) {
            try {
                JSONObject input = inputQueue.take();
                Instant startTime = Instant.now();
                String inputId = (String) input.get("id");
                JSONObject data = (JSONObject) input.get("data");
                Long prevDuration = (Long) input.get("duration");

                Pair<Operator, ArrayList<Pair<String, Operator>>> processingTriple = this.relation.get(inputId);

                if(processingTriple != null) {
                    if(!outputted.containsKey(inputId)) {
                        outputted.put(inputId, true);
                    }

                    Aggregation op = (Aggregation) processingTriple.getLeft();
                    op.inputData(data);
                    int outputRate = op.getOutputRate();

                    boolean isOutputted = outputted.get(inputId);

                    if(outputRate > 0 && isOutputted) {
                        outputted.put(inputId, false);
                        Long duration = Duration.between(startTime, Instant.now()).toMillis();

                        execService.schedule(() -> {
                            outputted.put(inputId, true);
                            ArrayList<Pair<String, Operator>> outputPairs = processingTriple.getRight();
                            try {
                                JSONObject result = op.getOutput();
                                if(result != null) {
                                    for (Pair<String, Operator> outputPair : outputPairs) {
                                        String outputId = outputPair.getLeft();
                                        Class nextOp = outputPair.getRight().getClass();
                                        JSONObject output = new JSONObject();
                                        output.put("id", outputId);
                                        output.put("data", result);
                                        output.put("duration", prevDuration + duration);
                                        otherInputQueues.get(nextOp).add(output);
                                    }
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }, outputRate, TimeUnit.SECONDS);
                    } else if(outputRate == 0 || (outputRate == -1 && op.tumbleCounterFull())) {
                        //processExecService.execute(() -> {
                            try {
                                JSONObject result = op.getOutput();
                                Long duration = Duration.between(startTime, Instant.now()).toMillis();

                                ArrayList<Pair<String, Operator>> outputPairs = processingTriple.getRight();
                                for (Pair<String, Operator> outputPair : outputPairs) {
                                    String outputId = outputPair.getLeft();
                                    Class nextOp = outputPair.getRight().getClass();
                                    JSONObject output = new JSONObject();
                                    output.put("id", outputId);
                                    output.put("data", result);
                                    output.put("duration", prevDuration + duration);
                                    otherInputQueues.get(nextOp).add(output);
                                }
                            } catch(Exception e) {
                                e.printStackTrace();
                            }
                        //});
                    }
                }

                Long durationFinal = Duration.between(startTime, Instant.now()).toMillis();
                Benchmark.addAggregationTime(durationFinal);

            } catch (Exception e) {
                e.printStackTrace();
            }


        }
    }
}
