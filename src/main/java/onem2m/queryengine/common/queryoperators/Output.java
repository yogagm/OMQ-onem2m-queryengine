package onem2m.queryengine.common.queryoperators;

import org.json.simple.JSONObject;

import java.util.HashMap;

public class Output implements Operator {
    private static final String abbreviation = "output";
    String queryId;
    public HashMap<String,String> columnNamingRules = new HashMap<>();

    public Output(String queryId, HashMap<String, String> mapping) {
        this.queryId = queryId;
        this.columnNamingRules = mapping;

    }



    public JSONObject renameColumns(JSONObject input) {
        JSONObject output = new JSONObject();
        for(Object key: input.keySet()) {
            String columnName = (String) key;
            String[] columnNameSplit = columnName.split("\\.", 2);
            String columnNameToFind = columnNameSplit[0];
            if(columnNamingRules.containsKey(columnNameToFind)) {
                String newColumnName = columnNamingRules.get(columnNameToFind);
                if(columnNameSplit.length > 1) {
                    newColumnName += "." + columnNameSplit[1];
                }
                output.put(newColumnName, input.get(key));
            } else {
                output.put(key, input.get(key));
            }
        }

        return output;
    }


    public String getQueryId() {
        return queryId;
    }

    @Override
    public JSONObject toJSON() {
        return null;
    }

    @Override
    public String getAbbreviation() {
        return abbreviation;
    }

    @Override
    public void adjustArgsFieldNaming(HashMap<String, String> columnNameMapping) {

    }
}
