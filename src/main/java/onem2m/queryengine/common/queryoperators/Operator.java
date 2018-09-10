package onem2m.queryengine.common.queryoperators;

import org.json.simple.JSONObject;

import java.util.HashMap;

public interface Operator {
    JSONObject toJSON();
    String getAbbreviation();

    void adjustArgsFieldNaming(HashMap<String, String> columnNameMapping);
}
