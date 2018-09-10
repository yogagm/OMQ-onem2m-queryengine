package onem2m.queryengine.common.threads;

import onem2m.queryengine.common.queryoperators.Filter;
import onem2m.queryengine.common.queryoperators.Operator;
import onem2m.queryengine.processorng.Benchmark;
import org.json.simple.JSONObject;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class FilterOpThread extends  SingleInputOpThread {
    public FilterOpThread(LinkedBlockingQueue<JSONObject> inputQueue, HashMap<Class, LinkedBlockingQueue<JSONObject>> otherInputQueues) {
        super(inputQueue, otherInputQueues);
    }

    @Override
    protected void benchmarkTiming(Long duration) {
        Benchmark.addFilterTime(duration);
    }

    @Override
    public JSONObject process(Object data, Operator processingOp) throws InvocationTargetException, IllegalAccessException {
        Filter op = (Filter) processingOp;
        if(op.passData((JSONObject) data)) {
            return (JSONObject) data;
        } else {
            // TODO: For benchmark purpose, so the output count still consistent, remove this for production
            return (JSONObject) data;
        }
    }
}
