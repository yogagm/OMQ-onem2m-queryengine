package onem2m.queryengine.common.queryoperators;

import org.json.simple.JSONObject;

import java.util.HashMap;

public class ExternalQueryDataSource implements DataSource {
    private static final String abbreviation = "_external_query";
    private String sourceQe;
    private String sourceColumnName = null;
    private String columnName;

    public ExternalQueryDataSource(String columnName, JSONObject opVars) {
        this.columnName = columnName;

        if(opVars.containsKey("source_qe")) {
            this.sourceQe = ((String) opVars.get("source_qe"));
        }

        if(opVars.containsKey("source_column_name")) {
            this.sourceColumnName = ((String) opVars.get("source_column_name"));
        }
    }

    public ExternalQueryDataSource(String sourceQe, String columnName) {
        this.columnName = columnName;
        this.sourceQe = sourceQe;
    }

    public String getSourceQe() {
        return sourceQe;
    }

    public String getName() {
        if(this.sourceColumnName == null) {
            return null;
        } else {
            return "extquery:" + sourceQe + ":" + sourceColumnName;
        }

    }


    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getSourceColumnName() {
        return sourceColumnName;
    }

    public void setSourceColumnName(String sourceColumnName) {
        this.sourceColumnName = sourceColumnName;
    }

    @Override
    public JSONObject toJSON() {
        JSONObject output = new JSONObject();
        JSONObject extQuery = new JSONObject();
        extQuery.put("source_qe", this.sourceQe);
        extQuery.put("source_column_name", this.sourceColumnName);
        output.put("_external_query", extQuery);
        return output;
    }

    @Override
    public String getAbbreviation() {
        return abbreviation;
    }

    @Override
    public void adjustArgsFieldNaming(HashMap<String, String> columnNameMapping) {

    }

}
