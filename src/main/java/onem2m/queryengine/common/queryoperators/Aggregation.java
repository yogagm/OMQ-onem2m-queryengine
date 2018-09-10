package onem2m.queryengine.common.queryoperators;

import org.json.simple.JSONObject;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Random;
import java.util.logging.Logger;

public class Aggregation implements Operator {
    private static final String abbreviation = "aggregate";

    private Method aggrFunc;
    private String funcName;
    private Integer windowSize;
    private Integer inputN = 0;
    private Integer outputRate;
    private JSONObject output = new JSONObject();
    private boolean firstData = true;
    Random rand = new Random();
    //Class dataType = null;
    private final static Logger LOGGER = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);

    HashMap<String,SlidingWindow> buffer = new HashMap<>();

    public Double latest(String columnName) {
        return this.buffer.get(columnName).get()[0];
    }

    // For benchmark purpose only
    public Double randomCalc(String columnName) {
        Integer sum = 0;
        for(int i=0;i<100;i++) {
            sum += rand.nextInt(50);
        }

        Double result = sum / 100.0;
        return result;
    }

    public double mean(String columnName) {
        //OptionalDouble mean = Arrays.stream(this.buffer.get(columnName).storage).average();
        double[] arr = this.buffer.get(columnName).storage;
        double sum = 0;
        for(double x: arr) {
            sum += x;
        }
        return (sum/arr.length);
    }

    public double sum(String columnName) {
        double sum = Arrays.stream(this.buffer.get(columnName).storage).sum();
        /*double[] arr = this.buffer.get(columnName).storage;
        double sum = 0;
        for(double x: arr) {
            sum += x;
        }*/
        return (sum);
    }

    public double min(String columnName) {
        double[] arr = this.buffer.get(columnName).storage;
        double min = 100000;
        for(double x: arr) {
            if(x < min) {
                min = x;
            }
        }
        return min;
    }

    public double max(String columnName) {
        double[] arr = this.buffer.get(columnName).storage;
        double max = -100000;
        for(double x: arr) {
            if(x > max) {
                max = x;
            }
        }
        return max;
    }

    public Aggregation(JSONObject opVars) throws NoSuchMethodException {

        // Operator Query Initialization
        if(opVars.containsKey("function")) {
            this.aggrFunc = Aggregation.class.getMethod((String) opVars.get("function"), String.class);
            this.funcName = (String) opVars.get("function");
        } else {
            this.aggrFunc = Aggregation.class.getMethod("latest", String.class);
            this.funcName = "latest";
        }



        if(opVars.containsKey("window_size")) {
            this.windowSize = ((Long) opVars.get("window_size")).intValue();
        } else {
            this.windowSize = 1;
        }

        if(opVars.containsKey("output_rate")) {
            this.outputRate = ((Long) opVars.get("output_rate")).intValue();
        } else {
            this.outputRate = -1;
        }


    }

    public void inputData(JSONObject input) {
        if(this.firstData) {
            for(Object key: input.keySet()) {
                String columnName = (String) key;
                if(!this.buffer.containsKey(columnName)) {
                    this.buffer.put(columnName, new SlidingWindow(this.windowSize));
                    output.put(columnName, null);
                }
            }
            this.firstData = false;
        }

        for(Object key: input.keySet()) {
            String columnName = (String) key;
            if(input.get(key) instanceof Long) {
                Long data = (Long) input.get(key);
                this.buffer.get(columnName).put(data.doubleValue());
            } else if(input.get(key) instanceof Double) {
                Double data = (Double) input.get(key);
                this.buffer.get(columnName).put(data);
            }


        }

        inputN++;
    }

    public JSONObject getOutput() throws InvocationTargetException, IllegalAccessException {
        for(Object key: output.keySet()) {
            Double result = (Double) this.aggrFunc.invoke(this, (String) key);
            output.put(key, result);
        }
        return output;
    }

    @Override
    public String toString() {
        return "aggregate-" + this.funcName;
    }

    @Override
    public boolean equals(Object obj) {
        Aggregation that = (Aggregation) obj;
        boolean equal = true;

        if(!this.funcName.equals(that.funcName)) {
            equal = false;
        }

        if(!this.outputRate.equals(that.outputRate)) {
            equal = false;
        }

        if(!this.windowSize.equals(that.windowSize)) {
            equal = false;
        }

        return equal;
    }

    @Override
    public JSONObject toJSON() {
        JSONObject opJSON = new JSONObject();
        opJSON.put("function", funcName);
        opJSON.put("window_size", windowSize);
        opJSON.put("output_rate", outputRate);
        return opJSON;
    }

    @Override
    public String getAbbreviation() {
        return abbreviation;
    }

    @Override
    public void adjustArgsFieldNaming(HashMap<String, String> columnNameMapping) {

    }

    public Integer getOutputRate() {
        return outputRate;
    }

    public boolean tumbleCounterFull() {
        return (inputN % windowSize == 0);
    }
}
