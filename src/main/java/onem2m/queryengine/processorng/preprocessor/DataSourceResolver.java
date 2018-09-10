package onem2m.queryengine.processorng.preprocessor;

import onem2m.queryengine.processorng.Global;
import org.json.simple.JSONObject;

public class DataSourceResolver {
    public static boolean existInMetadataMapping(String containerName, String criteriaKey, String criteriaValue) {
        JSONObject metadata = (JSONObject) Global.metadataMapping.get(containerName);
        if(metadata.containsKey(criteriaKey) && metadata.containsValue(criteriaValue)) {
            return true;
        }

        return false;
    }


    public static JSONObject resolveQuery(JSONObject newQuery) {
        JSONObject dataSources = (JSONObject) newQuery.get("select");
        for(Object columnName: dataSources.keySet()) {
            // Data source searching
            String resultContainerName = null;
            JSONObject dataSource = (JSONObject) dataSources.get(columnName);
            JSONObject criterias = (JSONObject) dataSource.get("source");
            for(Object containerName: Global.metadataMapping.keySet()) {
                String containerNameX = (String) containerName;
                int matchesCriteria = 0;
                for(Object criteriaKey: criterias.keySet()) {
                    String criteriaKeyX = (String) criteriaKey;
                    String criteriaValue = (String) criterias.get(criteriaKey);
                    if(existInMetadataMapping(containerNameX, criteriaKeyX, criteriaValue)) {
                        matchesCriteria++;
                    }
                }

                if(matchesCriteria == criterias.size()) {
                    // TEMP: Only consider one unique data source at this moment
                    resultContainerName = containerNameX;
                }
            }

            // Data source changing
            dataSource.remove("source");
            dataSource.put("_containers", resultContainerName);
        }

        return newQuery;
    }
}
