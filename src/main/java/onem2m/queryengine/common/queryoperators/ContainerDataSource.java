package onem2m.queryengine.common.queryoperators;


import org.json.simple.JSONObject;

import java.util.HashMap;

public class ContainerDataSource implements DataSource {
    private static final String abbreviation = "_containers";
    private String container;
    private String columnName;
    private boolean localQeExist = false;

    public ContainerDataSource(String columnName, String container) {
        this.columnName = columnName;
        this.container = container;


    }

    public String getContainer() {
        return container;
    }

    public String getName() {
        return "container:" + container;
    }

    public String getColumnName() {
        return columnName;
    }




    public boolean isLocalQeExist() {
        return localQeExist;
    }

    public void setLocalQeExist(boolean localQeExist) {
        this.localQeExist = localQeExist;
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof ContainerDataSource) {
            ContainerDataSource that = (ContainerDataSource) obj;
            boolean equal = true;
            if(!this.container.equals(that.container)) {
                equal = false;
            }

            return equal;
        } else {
            return false;
        }


    }

    @Override
    public String toString() {
        return "containerDataSource";
    }

    @Override
    public JSONObject toJSON() {
        JSONObject output = new JSONObject();
        output.put("_containers", this.container);
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
