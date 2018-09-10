package onem2m.queryengine.common.queryoperators;

import org.json.simple.JSONObject;

import java.util.HashMap;

public class Join implements Operator {
    private static final String abbreviation = "join";
    @Override
    public boolean equals(Object obj) {
        if(obj instanceof Join) {
            return true;
        }

        return false;
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
