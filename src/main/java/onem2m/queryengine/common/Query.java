package onem2m.queryengine.common;


import onem2m.queryengine.common.queryoperators.*;
import org.apache.commons.lang3.tuple.Pair;
import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.logging.Logger;

public class Query  {
    protected String id;
    protected static ArrayList<Query> queries = new ArrayList<>();
    protected ArrayList<Pair<DataSource, OperatorChain>> preJoin = new ArrayList<>();
    protected Join joinOp = new Join();
    protected OperatorChain postJoin = new OperatorChain();
    protected Output outputOp = null;
    HashMap<String,String> columnNameMapping = new HashMap<>();
    HashMap<String,String> reverseColumnNameMapping = new HashMap<>();

    private final static Logger LOGGER = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);

    public Query(String id) {
        this.id = id;
        this.outputOp = new Output(this.id, reverseColumnNameMapping);
        queries.add(this);
    }


    public void addColumnNameMapping(String columnName, String mappedTo) {
        columnNameMapping.put(columnName, mappedTo);
        reverseColumnNameMapping.put(mappedTo, columnName);
    }

    public void addPreJoin(DataSource ds, OperatorChain op) {
        preJoin.add(Pair.of(ds, op));

        // Additionally
        if(ds instanceof ContainerDataSource) {
            columnNameMapping.put(ds.getColumnName(), ds.getName());
        }
    }

    public void setPostJoin(OperatorChain op) {
        this.postJoin = op;
    }

    public ArrayList<Pair<DataSource, OperatorChain>> getPreJoin() {
        return this.preJoin;
    }

    public OperatorChain getPostJoin() {
        return this.postJoin;
    }




    public String getId() {
        return id;
    }

    public void addPostJoin(Operator op) {
        this.postJoin.addOperator(op);
    }


    public HashMap<String, String> getEqualPairs(Object obj) {
        Query toCompare = (Query) obj;

        HashMap<String, String> equalPairs = new HashMap<>();
        if(this.preJoin.size() == toCompare.preJoin.size()) {
            int expectedDsSize = this.preJoin.size();
            for(Pair<DataSource, OperatorChain> thisPair: this.preJoin) {
                for(Pair<DataSource, OperatorChain> thatPair: this.preJoin) {
                    DataSource thisDs = thisPair.getLeft();
                    DataSource thatDs = thatPair.getLeft();

                    OperatorChain thisOps = thisPair.getRight();
                    OperatorChain thatOps = thatPair.getRight();
                    if(thisDs.equals(thatDs) && thisOps.equals(thatOps)) {
                        if(thisDs instanceof ContainerDataSource && thatDs instanceof ContainerDataSource) {
                            ContainerDataSource thisDsX = (ContainerDataSource) thisDs;
                            ContainerDataSource thatDsX = (ContainerDataSource) thatDs;
                            equalPairs.put(thisDsX.getColumnName(), thatDsX.getColumnName());
                        }
                    }
                }
            }

            // No partial similarity
            if(equalPairs.size() != expectedDsSize) {
                return null;
            } else {
                return equalPairs;
            }
        } else {
            return null;
        }
    }

    @Override
    public boolean equals(Object obj) {
        Query toCompare = (Query) obj;

        boolean equal = true;

        // Compare preJoin
        if(this.preJoin.size() == toCompare.preJoin.size()) {
            int expectedDsSize = this.preJoin.size();
            ArrayList<Pair<Pair<DataSource, OperatorChain>, Pair<DataSource, OperatorChain>>> pairs = new ArrayList<>();
            for(Pair<DataSource, OperatorChain> thisPair: this.preJoin) {
                for(Pair<DataSource, OperatorChain> thatPair: this.preJoin) {
                    DataSource thisDs = thisPair.getLeft();
                    DataSource thatDs = thatPair.getLeft();

                    OperatorChain thisOps = thisPair.getRight();
                    OperatorChain thatOps = thatPair.getRight();
                    if(thisDs.equals(thatDs) && thisOps.equals(thatOps)) {
                        pairs.add(Pair.of(thisPair, thatPair));
                    }
                }
            }

            // No partial similarity

            if(pairs.size() != expectedDsSize) {
                equal = false;
            }
        } else {
            equal = false;
        }

        // Compare postJoin
        if(!this.postJoin.equals(toCompare.postJoin)) {
            equal = false;
        }

        return equal;
    }

    @Override
    public String toString() {
        String output = "";

        output += "== preJoin ==\n";
        for(Pair<DataSource, OperatorChain> pair: this.preJoin) {
            output += "* " + pair.getLeft().getName() +": ";
            for(Operator op: pair.getRight().getOperators()) {
                output += op.toString() + ", ";
            }
            output += "\n";
        }
        output += "== postJoin ==\n";
        for(Operator op: this.postJoin.getOperators()) {
            output += "* " + op.toString() + "\n";
        }

        return output;
    }

    public void setId(String id) {
        this.id = id;
    }

    public JSONObject toJSON() {
        JSONObject output = new JSONObject();
        output.put("id", this.id);

        JSONObject select = new JSONObject();
        for (Pair<DataSource, OperatorChain> pair : preJoin) {
            JSONObject preJoin = new JSONObject();
            DataSource ds = pair.getLeft();
            OperatorChain ops = pair.getRight();
            String columnName = ds.getColumnName();
            preJoin.putAll(ds.toJSON());
            preJoin.putAll(ops.toJSON());
            select.put(columnName, preJoin);
        }
        output.put("select", select);


        output.putAll(postJoin.toJSON());

        return output;

    }

    public Query(JSONObject query) throws NoSuchMethodException {
        this.id = (String) query.get("id");
        LOGGER.info("Receiving a new query: "+  this.id);
        this.outputOp = new Output(this.id, reverseColumnNameMapping);

        // pre join
        JSONObject preJoins = (JSONObject) query.get("select");
        for(Object columnKey: preJoins.keySet()) {
            String columnName = (String) columnKey;
            DataSource ds = null;
            OperatorChain ops = new OperatorChain();
            JSONObject preJoinOp = (JSONObject) preJoins.get(columnName);
            SortedSet preJoinOpX = new TreeSet(preJoinOp.keySet());
            for(Object opKey: preJoinOpX) {
                String opNameFull = (String) opKey;
                Object opValues = preJoinOp.get(opNameFull);
                String[] opNameParts = opNameFull.split("-");
                String opName = "";
                if(opNameParts.length == 2) {
                    opName = opNameParts[1];
                } else if(opNameParts.length == 1) {
                    opName = opNameParts[0];
                }

                switch(opName) {
                    // TODO: Implement data source resolver module here
                    case "data_source":
                        break;

                    case "_containers":
                        ds = new ContainerDataSource(columnName, (String) opValues);
                        this.addColumnNameMapping(columnName, ds.getName());
                        break;

                    case "_external_query":
                        ds = new ExternalQueryDataSource(columnName, (JSONObject) opValues);
                        //this.addColumnNameMapping(columnName, ds.getName());      // Oops, dont do any mapping yet if there is no clear sourceColumnName
                        break;

                    case "aggregate":
                        Aggregation aggr = new Aggregation((JSONObject) opValues);
                        ops.addOperator(aggr);
                        break;

                    case "transform":
                        JSONObject opValuesX = (JSONObject) opValues;
                        if(!opValuesX.containsKey("output_column")) {
                            opValuesX.put("output_column", columnName);
                        }
                        Transformation tran = new Transformation(opValuesX);
                        ops.addOperator(tran);
                        break;

                    case "filter":
                        Filter filter = new Filter((JSONObject) opValues);
                        ops.addOperator(filter);
                        break;
                }
            }

            this.addPreJoin(ds, ops);
        }

        // Post Join
        OperatorChain postOps = new OperatorChain();
        SortedSet postOpsX = new TreeSet(query.keySet());
        for(Object columnKey: postOpsX) {
            String columnNameFull = (String) columnKey;
            if(columnNameFull != "select") {
                Object opValues = query.get(columnNameFull);
                String[] columnNameParts = columnNameFull.split("-");
                String opName = "";
                if(columnNameParts.length == 2) {
                    opName = columnNameParts[1];
                } else if(columnNameParts.length == 1) {
                    opName = columnNameParts[0];
                }
                switch(opName) {
                    case "aggregate":
                        Aggregation aggr = new Aggregation((JSONObject) opValues);
                        postOps.addOperator(aggr);
                        break;

                    case "transform":
                        Transformation tran = new Transformation((JSONObject) opValues);
                        postOps.addOperator(tran);
                        break;

                    case "filter":
                        Filter filter = new Filter((JSONObject) opValues);
                        postOps.addOperator(filter);
                        break;
                }
            }
        }

        // Output
        this.setPostJoin(postOps);
        LOGGER.info("Received a new query: "+  this.id);
        queries.add(this);
        return;

    }



    @Deprecated
    public static String getFinalPreJoinColumnName(ContainerDataSource dsX, ArrayList<Operator> ops) {
        String name = dsX.getColumnName();
        for(Operator op: ops) {
            if(op instanceof Transformation) {
                Transformation opX = (Transformation) op;
                name = opX.getOutputColumnName();
            }
        }
        return name;
    }


    public static int getNumberOfQueries() {
        return queries.size();
    }
}
