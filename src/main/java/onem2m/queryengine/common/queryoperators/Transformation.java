package onem2m.queryengine.common.queryoperators;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.logging.Logger;

public class Transformation implements Operator {
    private static final String abbreviation = "transform";
    private final String funcName;
    private String outputColumnName;
    private String function;
    private JSONArray args;
    private Method tranFunc;
    private JSONObject tranArgs = new JSONObject();
    private boolean firstData;
    private boolean allColumnToBeProcessed = true;
    private ArrayList<String> toBeProcessedColumns = new ArrayList<>();

    private final static Logger LOGGER = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);

    public JSONObject nothing(JSONObject input) {
        return input;
    }
    public JSONObject mean(JSONObject input) {
        double sum = 0.0;
        for(Object key: input.keySet()) {
            Object inpData = input.get(key);
            if(inpData instanceof Long) {
                sum += ((Long) inpData).doubleValue();
            } else if(inpData instanceof Double) {
                sum += ((Double) inpData);
            }
        }
        sum /= input.size();

        JSONObject output = new JSONObject();
        output.put(this.outputColumnName, sum);
        return output;
    }

    public JSONObject sum(JSONObject input) {
        double sum = 0.0;
        for(Object key: input.keySet()) {
            Object inpData = input.get(key);
            if(inpData instanceof Long) {
                sum += ((Long) inpData).doubleValue();
            } else if(inpData instanceof Double) {
                sum += ((Double) inpData);
            }
        }

        JSONObject output = new JSONObject();
        output.put(this.outputColumnName, sum);
        return output;
    }

    public JSONObject percentage(JSONObject input) {
        JSONObject output = new JSONObject();
        double percentageMaximum = 100;
        if(this.args.size() == 1) {
            percentageMaximum = ((Long) this.args.get(0)).doubleValue();
        }
        for(Object key: input.keySet()) {
            output.put(key, (double) input.get(key) / percentageMaximum);
        }

        return output;
    }

    public String getOutputColumnName() {
        return this.outputColumnName;
    }



    public Transformation(JSONObject opVars) throws NoSuchMethodException {
        if(opVars.containsKey("function")) {
            this.tranFunc = Transformation.class.getMethod((String) opVars.get("function"), JSONObject.class);
            this.funcName = (String) opVars.get("function");
            // TODO: Add per-function column definition args into "toBeProcessedColumn"
        } else {
            this.tranFunc = Transformation.class.getMethod("nothing", JSONObject.class);
            this.funcName = "nothing";
        }

        if(opVars.containsKey("output_column")) {
            this.outputColumnName = (String) opVars.get("output_column");
        } else {
            this.outputColumnName = null;
        }

        if(opVars.containsKey("args")) {
            this.args = ((JSONArray) opVars.get("args"));
        } else {
            this.args = new JSONArray();
        }
    }



    public JSONObject processData(Object obj) throws InvocationTargetException, IllegalAccessException {
        JSONObject input = (JSONObject) obj;
        if(this.firstData) {
            if(this.outputColumnName == null) {
                // Find right column name, uses first column found
                Set keySet = input.keySet();
                if(keySet.size() > 0) {
                    this.outputColumnName = (String) keySet.toArray()[0];
                } else {
                    this.outputColumnName = "output";
                }
            }
            this.firstData = false;
        }

        JSONObject output = (JSONObject) this.tranFunc.invoke(this, input);
        return output;

    }

    public boolean isAllColumnToBeProcessed() {
        return allColumnToBeProcessed;
    }


    public ArrayList<String> getToBeProcessedColumns() {
        return toBeProcessedColumns;
    }

    @Override
    public String toString() {
        return "transform";
    }

    @Override
    public boolean equals(Object obj) {
        Transformation that = (Transformation) obj;
        boolean equal = true;

        if(!this.tranFunc.equals(that.tranFunc)) {
            equal = false;
        }

        if(!this.tranArgs.equals(that.tranArgs)) {
            equal = false;
        }

        if(!this.outputColumnName.equals(that.outputColumnName)) {
            equal = false;
        }

        return equal;
    }

    @Override
    public JSONObject toJSON() {
        JSONObject opJSON = new JSONObject();
        opJSON.put("function", funcName);
        opJSON.put("args", args);
        opJSON.put("output_column", outputColumnName);
        return opJSON;
    }


    @Override
    public String getAbbreviation() {
        return abbreviation;
    }

    @Override
    public void adjustArgsFieldNaming(HashMap<String, String> columnNameMapping) {
        JSONArray newArgs = new JSONArray();
        if(args != null) {
            for(Object arg: args) {
                if(arg instanceof String) {
                    String argX = (String) arg;
                    String[] argXSplit = argX.split("\\.", 2);
                    if(columnNameMapping.containsKey(argXSplit[0])) {
                        String newArg = columnNameMapping.get(argXSplit[0]);
                        if(argXSplit.length > 1) {
                            newArg += "." + argXSplit[1];
                        }
                        newArgs.add(newArg);
                    } else {
                        newArgs.add(arg);
                    }
                } else {
                    newArgs.add(arg);
                }
            }
        }
        this.args = newArgs;
    }
}
