package onem2m.queryengine.common.threads;

import onem2m.queryengine.common.queryoperators.Operator;
import onem2m.queryengine.common.queryoperators.Transformation;
import onem2m.queryengine.processorng.Benchmark;
import org.json.simple.JSONObject;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class TransformationOpThread extends SingleInputOpThread {


    public TransformationOpThread(LinkedBlockingQueue<JSONObject> inputQueue, HashMap<Class, LinkedBlockingQueue<JSONObject>> otherInputQueues) {
        super(inputQueue, otherInputQueues);
    }

    @Override
    protected void benchmarkTiming(Long duration) {
        Benchmark.addTranTime(duration);
    }

    @Override
    public JSONObject process(Object data, Operator processingOp) throws InvocationTargetException, IllegalAccessException {
        Transformation op = (Transformation) processingOp;
        JSONObject result = op.processData(data);
        return result;
    }
}
