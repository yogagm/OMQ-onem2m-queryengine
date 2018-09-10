package onem2m.queryengine.common.queryoperators;

import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;

public class OperatorChain {
    ArrayList<Operator> operators = new ArrayList<Operator>();


    public void addOperator(Operator operator) {
        operators.add(operator);
    }

    public ArrayList<Operator> getOperators() {
        return this.operators;
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof OperatorChain) {
            boolean equal = true;
            ArrayList<Operator> thisOps = this.operators;
            ArrayList<Operator> thatOps = ((OperatorChain) obj).operators;
            if(thisOps.size() == thatOps.size()) {
                for(int i=0;i<thisOps.size();i++) {
                    if(!thisOps.get(i).equals(thatOps.get(i))) {
                        equal = false;
                    }
                }
            } else {
                equal = false;
            }

            return equal;
        } else {
            return false;
        }
    }

    public JSONObject toJSON() {
        JSONObject output = new JSONObject();

        int i = 0;
        for(Operator op: operators) {
            JSONObject opJSON = op.toJSON();
            output.put(i + "-" +  op.getAbbreviation(), opJSON);
            i++;
        }

        return output;
    }

    public void adjustArgsFieldNaming(HashMap<String, String> columnNameMapping) {
        for(Operator op: operators) {
            op.adjustArgsFieldNaming(columnNameMapping);
        }
    }
}
