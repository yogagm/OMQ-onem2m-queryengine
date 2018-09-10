package onem2m.queryengine.common.queryoperators;

import org.json.simple.JSONObject;

public interface DataSource extends Operator {
    String getName();
    String getColumnName();
    JSONObject toJSON();
}
