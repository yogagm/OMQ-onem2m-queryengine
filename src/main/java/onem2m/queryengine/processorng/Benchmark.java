package onem2m.queryengine.processorng;

import onem2m.queryengine.common.Query;
import org.json.simple.JSONObject;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;

public class Benchmark extends Thread {
    // CONFIGURABLE
    public static int inputTputTresshold = 100127;
    public static int nQuery = 1;
    public static int tputTresshold = 17 * nQuery;
    public static int nTest = 10;

    // NON-CONFIGURABLE
    public static int tput = 0;
    public static int inputTput = 0;
    public static Instant startTime;
    private static double inputTputResult;
    private static double tputResult;
    public static double outputCountTotal = 0.0;
    public static int testN = 0;
    public static ArrayList<JSONObject> benchmarkResultsAll = new ArrayList<>();



    public static void startTimer() {
        //System.out.println("Starting timer");
        startTime = Instant.now();
    }


    public static void increamentInputTput() {
        inputTput++;

        //System.out.println(inputTput);

        if(inputTput == inputTputTresshold) {

            Long ns = Duration.between(startTime, Instant.now()).toMillis();
            inputTputResult = (inputTputTresshold * 1000) / ns.doubleValue();
            inputTput = 0;
            DataConsumer.timerStarted = false;
        }
    }

    public static double processingTimeSum = 0.0;
    public static int processingTimeCount = 0;

    public static void displayBenchmarkStatus() {
        JSONObject benchmarkResults = new JSONObject();
        benchmarkResults.put("n_queries", (double) Query.getNumberOfQueries());
        benchmarkResults.put("tput", tputResult);
        benchmarkResults.put("output_count", new Double(processingTimeCount));

        benchmarkResults.put("ds_sum", new Double(dsTimeSum));
        benchmarkResults.put("aggr_sum", new Double(aggrTimeSum));
        benchmarkResults.put("filter_sum", new Double(filterTimeSum));
        benchmarkResults.put("tran_sum", new Double(tranTimeSum));
        benchmarkResults.put("join_sum", new Double(joinTimeSum));
        benchmarkResults.put("output_sum", new Double(outpTimeSum));
        benchmarkResults.put("data_consumer_sum", new Double(dataConsumerTimeSum));

        System.out.println(benchmarkResults.toJSONString());

        increaseTestN(benchmarkResults);

    }

    public static void resetBenchmark() {
        tput = 0;

        dataConsumerTimeSum = 0.0;
        dataConsumerTimeCount = 0;
        processingTimeSum = 0.0;
        processingTimeCount = 0;
        aggrTimeSum = 0.0;
        aggrTimeCount = 0;
        dsTimeSum = 0.0;
        dsTimeCount = 0;
        filterTimeSum = 0.0;
        filterTimeCount = 0;
        tranTimeSum = 0.0;
        tranTimeCount = 0;
        joinTimeSum = 0.0;
        joinTimeCount = 0;
        outpTimeSum = 0.0;
        outpTimeCount = 0;

    }
    public static void increamentTput() {

        tput++;
        if(tput == tputTresshold) {
            //System.out.println("Stopping Timer");
            Long ns = Duration.between(startTime, Instant.now()).toMillis();
            tputResult = (inputTputTresshold * 1000) / ns.doubleValue();
            displayBenchmarkStatus();
            resetBenchmark();
        }
    }

    private static void increaseTestN(JSONObject benchmarkResults) {
        benchmarkResultsAll.add(benchmarkResults);
        testN++;
        int totalTestN = testN;
        if(testN == totalTestN + 1) {
            HashMap<String, Double> aggregates = new HashMap<>();
            boolean firstData = true;
            for(JSONObject bench: benchmarkResultsAll) {
                if(firstData) {
                    for(Object key: bench.keySet()) {
                        aggregates.put((String) key, null);
                    }
                    firstData = false;
                } else {
                    for(Object key: bench.keySet()) {
                        if(aggregates.get(key) == null) {
                            aggregates.put((String) key, (double) bench.get(key));
                        } else {
                            double meas = aggregates.get(key);
                            meas += (double) bench.get(key);
                            aggregates.put((String) key, meas);
                        }
                    }
                }
            }

            System.out.println("===========================");
            for(String key: aggregates.keySet()) {
                double result = aggregates.get(key) / (totalTestN-1);
                System.out.println(key + ": " + result);
            }
            System.out.println("===========================");

        }
    }


    public static void addProcessingTime(Long duration) {
        processingTimeSum += duration;
        processingTimeCount++;
        outputCountTotal++;
    }

    public static double dataConsumerTimeSum = 0.0;
    public static int dataConsumerTimeCount = 0;
    public static void addDataConsumerTime(Long duration) {
        dataConsumerTimeSum += duration;
        dataConsumerTimeCount++;
    }

    public static double aggrTimeSum = 0.0;
    public static int aggrTimeCount = 0;
    public static void addAggregationTime(Long duration) {
        aggrTimeSum += duration;
        aggrTimeCount++;
    }

    public static double dsTimeSum = 0.0;
    public static int dsTimeCount = 0;
    public static void addDsTime(Long duration) {
        dsTimeSum += duration;
        dsTimeCount++;
    }

    public static double outpTimeSum = 0.0;
    public static int outpTimeCount = 0;
    public static void addOutputTime(Long duration) {
        outpTimeSum += duration;
        outpTimeCount++;
    }

    public static double joinTimeSum = 0.0;
    public static int joinTimeCount = 0;
    public static void addJoinTime(Long duration) {
        joinTimeSum += duration;
        joinTimeCount++;
    }

    public static double tranTimeSum = 0.0;
    public static int tranTimeCount = 0;
    public static void addTranTime(Long duration) {
        tranTimeSum += duration;
        tranTimeCount++;
    }

    public static double filterTimeSum = 0.0;
    public static int filterTimeCount = 0;
    public static void addFilterTime(Long duration) {
        filterTimeSum += duration;
        filterTimeCount++;
    }
}
