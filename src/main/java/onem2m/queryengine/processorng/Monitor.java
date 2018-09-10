package onem2m.queryengine.processorng;

import org.json.simple.JSONObject;

public class Monitor extends Thread {
    @Override
    public void run() {
        JSONObject output = new JSONObject();
        try {
            while(true) {
                output.put("inputN", Benchmark.inputTput);

                output.put("dsQSize", OpThreadManager.dataSourceOpQueue.size());
                output.put("aggrQSize", OpThreadManager.aggrOpQueue.size());
                output.put("tranQSize", OpThreadManager.tranOpQueue.size());
                output.put("filterQSize", OpThreadManager.filterOpQueue.size());
                output.put("joinQSize", OpThreadManager.joinOpQueue.size());
                output.put("outputQSize", OpThreadManager.outputOpQueue.size());
                output.put("outputCount", Benchmark.outputCountTotal);
                
                //System.out.println(output.toJSONString());
                Thread.sleep(1000);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
