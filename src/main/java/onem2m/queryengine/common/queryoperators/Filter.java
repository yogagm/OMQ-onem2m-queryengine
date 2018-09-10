package onem2m.queryengine.common.queryoperators;

import onem2m.queryengine.common.queryoperators.filteroperators.And;
import onem2m.queryengine.common.queryoperators.filteroperators.Condition;
import onem2m.queryengine.common.queryoperators.filteroperators.FilterOperator;
import onem2m.queryengine.common.queryoperators.filteroperators.Or;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;

public class Filter implements Operator {
    private static final String abbreviation = "filter";
    private ArrayList<Condition> conditions = new ArrayList<>();
    private ArrayList<FilterOperator> filterOperators = new ArrayList<>();
    private ArrayList<String> toBeProcessedColumns = new ArrayList<>();

    public Filter(JSONObject opVars) {
        filterOperators.add(new And());
        if(opVars.containsKey("predicates")) {
           JSONArray predicates = (JSONArray) opVars.get("predicates");
           for(Object predicate: predicates) {
               String predicateStr = (String) predicate;

               if(predicateStr.equals("&")) {
                   filterOperators.add(new And());
               } else if(predicateStr.equals("|")) {
                   filterOperators.add(new Or());
               } else {
                   Condition newCondition = new Condition(predicateStr);
                   conditions.add(newCondition);
                   toBeProcessedColumns.add(newCondition.getColumnName());
               }
           }
        }
    }

    public boolean passData(JSONObject input) {
        boolean current = true;
        if(conditions.size() == filterOperators.size()) {
            for(int i=0;i<conditions.size();i++) {
                boolean conditionBool = conditions.get(i).operate(input);
                current = filterOperators.get(i).operate(current, conditionBool);
            }
        }

        return current;
    }

    public ArrayList<String> getToBeProcessedColumns() {
        return toBeProcessedColumns;
    }

    @Override
    public String toString() {
        return "filter";
    }

    @Override
    public boolean equals(Object obj) {
        Filter that = (Filter) obj;
        boolean equal = true;

        if(this.conditions.size() == that.conditions.size()) {
            for(int i=0;i<this.conditions.size();i++) {
                if(!this.conditions.get(i).equals(that.conditions.get(i))) {
                    equal = false;
                }
            }
        }

        if(this.filterOperators.size() == that.filterOperators.size()) {
            for(int i=0;i<this.filterOperators.size();i++) {
                if(!this.filterOperators.get(i).equals(that.filterOperators.get(i))) {
                    equal = false;
                }
            }
        }

        return equal;
    }

    @Override
    public JSONObject toJSON() {
        JSONObject opJSON = new JSONObject();
        JSONArray predicates = new JSONArray();
        if(conditions.size() == filterOperators.size()) {
            for(int j=0; j<conditions.size();j++) {
                if(j>0) {
                    predicates.add(filterOperators.get(j).toString());
                }
                predicates.add(conditions.get(j).toString());
            }
        }
        opJSON.put("predicates", predicates);
        return opJSON;
    }


    @Override
    public String getAbbreviation() {
        return abbreviation;
    }

    @Override
    public void adjustArgsFieldNaming(HashMap<String, String> columnNameMapping) {
        for(Condition condition: conditions) {
            condition.adjustArgsFieldNaming(columnNameMapping);
        }
    }
}
