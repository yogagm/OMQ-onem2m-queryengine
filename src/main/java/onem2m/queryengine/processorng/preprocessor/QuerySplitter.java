package onem2m.queryengine.processorng.preprocessor;

import onem2m.queryengine.common.QE;
import onem2m.queryengine.common.Query;
import onem2m.queryengine.common.SubQuery;
import onem2m.queryengine.common.queryoperators.*;
import onem2m.queryengine.processorng.Global;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.UUID;

public class QuerySplitter {
    // ** A much simpler query splitter **

    // Use the same concept as the original idea (post-join never go to local qe node)
    // With a new local qe implementation

    public static HashMap<QE, SubQuery> splitQuery(Query query) {
        HashMap<QE, SubQuery> subQueries = new HashMap<>();
        String queryId = (String) query.getId();
        // Create new sub-query for each local query engine
        subQueries.put(Global.mainQe, new SubQuery(queryId));
        for(String localQe: Global.localQes.keySet()) {
            subQueries.put(Global.localQes.get(localQe), new SubQuery(UUID.randomUUID().toString()));
        }


        // Read pre-join
        //HashMap<String, String> columnNameQE = new HashMap<>();
        for(Pair<DataSource, OperatorChain> preJoin: query.getPreJoin()) {
            DataSource ds = preJoin.getLeft();
            OperatorChain ops = preJoin.getRight();
            if(ds instanceof ContainerDataSource) {
                ContainerDataSource dsX = (ContainerDataSource) ds;
                QE localQe = QE.getQeOfDataSource(dsX);
                if(subQueries.containsKey(localQe)) {
                    // If there is exist local QE & within local QE query allowances, add this operator into local sub-query
                    int operatorCount = 0;
                    OperatorChain localOperators = new OperatorChain();
                    OperatorChain nonLocalOperators = new OperatorChain();
                    for(Operator op: ops.getOperators()) {
                        operatorCount++;
                        if(operatorCount <= Global.maxLocalOp) {
                            localOperators.getOperators().add(op);
                        } else {
                            nonLocalOperators.getOperators().add(op);
                        }
                    }
                    subQueries.get(localQe).addPreJoin(ds, localOperators);

                    // External query data source for main sub-queries
                    ExternalQueryDataSource exDs = new ExternalQueryDataSource(localQe.getQeName(), dsX.getColumnName());
                    subQueries.get(Global.mainQe).addPreJoin(exDs, nonLocalOperators);
                } else {
                    //  else, add this operator as main sub-queries
                    subQueries.get(Global.mainQe).addPreJoin(ds, ops);
                }


                //columnNameQE.put(Query.getFinalPreJoinColumnName(dsX, ops.getOperators()), localQe);
            }
        }

        // Read post-join
        // Post-join always in main-qe
        subQueries.get(Global.mainQe).setPostJoin(query.getPostJoin());

        // Remove subQueries which has no pre-join
        ArrayList<QE> toRemove = new ArrayList<>();
        for(QE qe: subQueries.keySet()) {
            SubQuery sq = subQueries.get(qe);
            if(sq.getPreJoin().size() == 0) {
                toRemove.add(qe);
            }
        }

        for(QE qe: toRemove) {
            subQueries.remove(qe);
        }

        for(QE qe: subQueries.keySet()) {
            subQueries.get(qe).adjustArgsFieldNaming();
        }


        return subQueries;
    }
}
