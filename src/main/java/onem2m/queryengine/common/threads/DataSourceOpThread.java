package onem2m.queryengine.common.threads;


import onem2m.queryengine.common.queryoperators.ContainerDataSource;
import onem2m.queryengine.common.queryoperators.DataSource;
import onem2m.queryengine.common.queryoperators.ExternalQueryDataSource;
import onem2m.queryengine.common.queryoperators.Operator;
import onem2m.queryengine.processorng.Benchmark;
import org.json.simple.JSONObject;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class DataSourceOpThread extends SingleInputOpThread {


    public DataSourceOpThread(LinkedBlockingQueue<JSONObject> inputQueue, HashMap<Class, LinkedBlockingQueue<JSONObject>> otherInputQueues) {
        super(inputQueue, otherInputQueues);
    }

    @Override
    protected void benchmarkTiming(Long duration) {
        Benchmark.addDsTime(duration);
    }

    @Override
    public JSONObject process(Object data, Operator processingOp) throws InvocationTargetException, IllegalAccessException {
        DataSource op = (DataSource) processingOp;
        JSONObject result = null;

        if(op.getClass() == ContainerDataSource.class) {
            String columnName = op.getName();
            result = new JSONObject();
            if(data instanceof JSONObject) {
                JSONObject dataX = (JSONObject) data;
                for(Object inputColumn: dataX.keySet()) {
                    result.put(columnName + "." + inputColumn, dataX.get(inputColumn));
                }
            } else {
                result.put(columnName, data);
            }

        } else if(op.getClass() == ExternalQueryDataSource.class) {
            // [NEED TESTING] TODO: (json-data-support) Change the ExternalQueryDataSource to assume the data is not only a single data
            String columnName = op.getColumnName();
            result = new JSONObject();
            if(data instanceof JSONObject) {
                JSONObject dataX = (JSONObject) data;
                for(Object inputColumn: dataX.keySet()) {
                    result.put(columnName + "." + inputColumn, dataX.get(inputColumn));
                }
            } else {
                result.put(columnName, data);
            }
        }
        return result;
    }


}
