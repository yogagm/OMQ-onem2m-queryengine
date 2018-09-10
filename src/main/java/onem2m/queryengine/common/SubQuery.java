package onem2m.queryengine.common;

import onem2m.queryengine.localprocessorng.LocalOpThreadManager;
import onem2m.queryengine.localprocessorng.LocalOutputOpThread;
import onem2m.queryengine.common.queryoperators.DataSource;
import onem2m.queryengine.common.queryoperators.ExternalQueryDataSource;
import onem2m.queryengine.common.queryoperators.Operator;
import onem2m.queryengine.common.queryoperators.OperatorChain;
import onem2m.queryengine.processorng.JoinOpThread;
import onem2m.queryengine.common.threads.OpThread;
import onem2m.queryengine.processorng.OpThreadManager;
import onem2m.queryengine.common.threads.SingleInputOpThread;
import org.apache.commons.lang3.tuple.Pair;
import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.UUID;
import java.util.logging.Logger;

public class SubQuery extends Query {
    private final static Logger LOGGER = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
    public SubQuery(String id) {
        super(id);
    }


    private static HashMap<String,String> checkConnectionExist(ArrayList<Pair<String, ArrayList<String>>> prevOpConnection, ArrayList<Pair<String, ArrayList<String>>> opConnection) {
        HashMap<String,String> result = new HashMap<>();
        for(Pair<String, ArrayList<String>> nextIdPair: opConnection) {
            String nextId = nextIdPair.getLeft();
            for(Pair<String, ArrayList<String>> prevIdPair: prevOpConnection) {
                ArrayList<String> prevIds = prevIdPair.getRight();
                if(prevIds.contains(nextId)) {
                    result.put(prevIdPair.getLeft(), nextId);
                }
            }
        }
        return result;
    }

    private static String checkJoinConnectionExist(ArrayList<String> prevIds, ArrayList<OpThread> prevOpThreads, HashMap<String, String> inputToJoinId) {
        HashMap<String, ArrayList<String>> seekResult = new HashMap<>();
        if(prevIds.size() == prevOpThreads.size()) {
            for(int i=0;i<prevIds.size();i++) {
                SingleInputOpThread prevOpThread = (SingleInputOpThread) prevOpThreads.get(i);
                Pair<Operator, ArrayList<Pair<String, Operator>>> connectionPair = prevOpThread.getInputConnection(prevIds.get(i));
                // TODO: There is null exception in connectionPair
                for(Pair<String, Operator> outputPair: connectionPair.getRight()) {
                    String inputId = outputPair.getLeft();
                    String joinId = inputToJoinId.get(inputId);
                    if(!seekResult.containsKey(joinId)) {
                        seekResult.put(joinId, new ArrayList<>());
                    }
                    seekResult.get(joinId).add(inputId);
                }
            }
        }

        for(String joinId: seekResult.keySet()) {
            if(seekResult.get(joinId).size() == prevIds.size()) {
                return joinId;
            }
        }

        return null;

    }


    public HashMap<String,String> provisionLocalQueryNg() {


        HashMap<String,String> outputMapping = new HashMap<>();
        LOGGER.info("Provisioning local query " + this.id);
        try {

            for (Pair<DataSource, OperatorChain> pair : preJoin) {
                DataSource ds = pair.getLeft();
                OperatorChain ops = pair.getRight();
                String columnName = ds.getColumnName();
                String dsName = ds.getName();
                String prevId = dsName;
                if (!LocalOpThreadManager.dataSourceOpThread.isInputExist(ds.getName())) {
                    LocalOpThreadManager.dataSourceOpThread.addInput(prevId);
                }
                OpThread prevOpThread = LocalOpThreadManager.dataSourceOpThread;
                ArrayList<Pair<String, ArrayList<String>>> prevOpConnection = ((SingleInputOpThread) prevOpThread).getOperatorConnections(ds);
                Operator prevOp = ds;


                boolean branched = false;
                for (Operator op : ops.getOperators()) {
                    // Is there any similar operator in the target thread
                    SingleInputOpThread opThread = (SingleInputOpThread) LocalOpThreadManager.getOpThread(op.getClass());
                    ArrayList<Pair<String, ArrayList<String>>> opConnection = opThread.getOperatorConnections(op);

                    // NOTE: Just setting connectionExist with null will ignore multi-query optimization entirely
                    HashMap<String,String> connectionExist = checkConnectionExist(prevOpConnection, opConnection);
                    if(!(connectionExist.isEmpty()) && !branched && connectionExist.containsKey(prevId)) {
                        // Connection exist, no need to create new OP for this
                        // Get existing inputId
                        prevId = connectionExist.get(prevId);
                        prevOpThread = opThread;
                        prevOp = op;
                        prevOpConnection = opConnection;
                    } else {
                        String inputId = UUID.randomUUID().toString();
                        opThread.addInput(inputId);
                        SingleInputOpThread prevOpThreadX = (SingleInputOpThread) prevOpThread;
                        prevOpThreadX.addConnection(prevId, inputId, prevOp, op);
                        prevOpThread = opThread;
                        prevId = inputId;
                        prevOp = op;
                        prevOpConnection = opConnection;
                        branched = true;
                    }
                }

                LocalOutputOpThread outputOpThread = LocalOpThreadManager.outputOpThread;
                if(branched == true) {
                    // If branched, create a new localOutputPoint
                    String localOutputOpInputId = UUID.randomUUID().toString();
                    outputOpThread.addInput(localOutputOpInputId, prevId);
                    SingleInputOpThread prevOpThreadX = (SingleInputOpThread) prevOpThread;
                    prevOpThreadX.addConnection(prevId, localOutputOpInputId, prevOp, outputOp);
                    outputMapping.put(columnName, localOutputOpInputId);
                } else {
                    // Else, use already existing localOutputPoint
                    if(outputOpThread.getInputId(prevId) != null) {
                        String localOutputOpInputId = outputOpThread.getInputId(prevId);
                        outputMapping.put(columnName, localOutputOpInputId);
                    } else {
                        String localOutputOpInputId = UUID.randomUUID().toString();
                        outputOpThread.addInput(localOutputOpInputId, prevId);
                        SingleInputOpThread prevOpThreadX = (SingleInputOpThread) prevOpThread;
                        prevOpThreadX.addConnection(prevId, localOutputOpInputId, prevOp, outputOp);
                        outputMapping.put(columnName, localOutputOpInputId);
                    }

                }



            }

            System.out.println(LocalOpThreadManager.dataSourceOpThread);
            System.out.println(LocalOpThreadManager.aggrOpThread);


        } catch(Exception e) {
            e.printStackTrace();
        }

        LOGGER.info("Output Mapping for new local query " + this.id + " : " + outputMapping);
        return outputMapping;
    }

    public boolean provisionQueryNg(){
        LOGGER.info("Provisioning query: " + this.id);
        // TODO: note down all involved Id for query stopping
        // Prejoin

        try {
            // TODO: Cancel query provisioning if one of external query does not have sourceColumnName

            ArrayList<OpThread> prevOpThreads = new ArrayList<>();
            ArrayList<String> prevIds = new ArrayList<>();
            ArrayList<Operator> prevOps = new ArrayList<>();
            ArrayList<ArrayList<Pair<String, ArrayList<String>>>> prevOpConnections = new ArrayList<>();
            boolean brancheds = false;
            for (Pair<DataSource, OperatorChain> pair : preJoin) {
                DataSource ds = pair.getLeft();
                OperatorChain ops = pair.getRight();

                String dsName = ds.getName();
                String prevId = dsName;
                if (!OpThreadManager.dataSourceOpThread.isInputExist(ds.getName())) {
                    OpThreadManager.dataSourceOpThread.addInput(prevId);
                }
                OpThread prevOpThread = OpThreadManager.dataSourceOpThread;
                ArrayList<Pair<String, ArrayList<String>>> prevOpConnection = ((SingleInputOpThread) prevOpThread).getOperatorConnections(ds);
                Operator prevOp = ds;

                boolean branched = false;
                if(ops.getOperators().size() == 0) {
                    branched = true;
                }
                for (Operator op : ops.getOperators()) {
                    // Is there any similar operator in the target thread
                    SingleInputOpThread opThread = (SingleInputOpThread) OpThreadManager.getOpThread(op.getClass());
                    ArrayList<Pair<String, ArrayList<String>>> opConnection = opThread.getOperatorConnections(op);

                    // NOTE: Just setting connectionExist with null will ignore multi-query optimization entirely
                    HashMap<String,String> connectionExist = checkConnectionExist(prevOpConnection, opConnection);
                    if(!connectionExist.isEmpty() && !branched && connectionExist.containsKey(prevId)) {
                        // Connection exist, no need to create new OP for this
                        // Get existing inputId
                        prevId = connectionExist.get(prevId);
                        prevOpThread = opThread;
                        prevOp = op;
                        prevOpConnection = opConnection;
                    } else {
                        String inputId = UUID.randomUUID().toString();
                        opThread.addInput(inputId);
                        SingleInputOpThread prevOpThreadX = (SingleInputOpThread) prevOpThread;
                        prevOpThreadX.addConnection(prevId, inputId, prevOp, op);
                        prevOpThread = opThread;
                        prevId = inputId;
                        prevOp = op;
                        prevOpConnection = opConnection;
                        branched = true;
                    }
                }

                prevOpConnections.add(prevOpConnection);
                prevOpThreads.add(prevOpThread);
                prevIds.add(prevId);
                prevOps.add(prevOp);
                brancheds = brancheds || branched;

            }


            String joinId = null;
            if(brancheds == true) {
                ArrayList<String> toJoinIds = new ArrayList<>();
                for(int i=0; i<preJoin.size(); i++) {
                    String toJoinId = UUID.randomUUID().toString();
                    toJoinIds.add(toJoinId);
                    SingleInputOpThread prevOpThreadX = (SingleInputOpThread) prevOpThreads.get(i);
                    //System.out.println("(" +  prevOp + ") Adding " + prevId + " --> " + toJoinId);
                    prevOpThreadX.addConnection(prevIds.get(i), toJoinId, prevOps.get(i), joinOp);
                }
                joinId = OpThreadManager.joinOpThread.addInput(toJoinIds);
            } else {
                HashMap<String, String> inputToJoinIdMapping = OpThreadManager.joinOpThread.getInputJoinIdMapping();
                joinId = checkJoinConnectionExist(prevIds, prevOpThreads, inputToJoinIdMapping);
            }

            if(joinId == null) {
                ArrayList<String> toJoinIds = new ArrayList<>();
                for(int i=0; i<preJoin.size(); i++) {
                    String toJoinId = UUID.randomUUID().toString();
                    toJoinIds.add(toJoinId);
                    SingleInputOpThread prevOpThreadX = (SingleInputOpThread) prevOpThreads.get(i);
                    //System.out.println("(" +  prevOp + ") Adding " + prevId + " --> " + toJoinId);
                    prevOpThreadX.addConnection(prevIds.get(i), toJoinId, prevOps.get(i), joinOp);
                }
                joinId = OpThreadManager.joinOpThread.addInput(toJoinIds);
            }

            // Join
            // Post-join
            // TODO: Add multi-query optimization in post-join
            String prevId = null;
            Operator prevOp = null;
            OpThread prevOpThread = OpThreadManager.joinOpThread;
            for (Operator op : this.postJoin.getOperators()) {
                String inputId = UUID.randomUUID().toString();
                SingleInputOpThread opThread = (SingleInputOpThread) OpThreadManager.getOpThread(op.getClass());
                opThread.addInput(inputId);
                if (prevOpThread.getClass() == JoinOpThread.class) {
                    JoinOpThread prevOpThreadX = (JoinOpThread) prevOpThread;
                    prevOpThreadX.addConnection(joinId, inputId, joinOp, op);
                } else {
                    SingleInputOpThread prevOpThreadX = (SingleInputOpThread) prevOpThread;
                    prevOpThreadX.addConnection(prevId, inputId, prevOp, op);
                }


                prevOpThread = opThread;
                prevId = inputId;
                prevOp = op;
            }

            // Output
            if (prevOpThread.getClass() == JoinOpThread.class) {
                String lastId = UUID.randomUUID().toString();
                OpThreadManager.outputOpThread.addInput(lastId, outputOp);
                JoinOpThread prevOpThreadX = (JoinOpThread) prevOpThread;
                prevOpThreadX.addConnection(joinId, lastId, joinOp, outputOp);
            } else {
                String lastId = UUID.randomUUID().toString();
                OpThreadManager.outputOpThread.addInput(lastId, outputOp);
                SingleInputOpThread prevOpThreadX = (SingleInputOpThread) prevOpThread;
                prevOpThreadX.addConnection(prevId, lastId, prevOp, outputOp);
            }

            LOGGER.info("Provisioned query: " + this.id);

        } catch(Exception e) {
            e.printStackTrace();
        }

        //System.out.println(OpThreadManager.dataSourceOpThread);
        //System.out.println(OpThreadManager.aggrOpThread);

        return true;
    }


    @Deprecated
    public boolean provisionQuery() {
        LOGGER.info("Provisioning query: " + this.id);
        // TODO: note down all involved Id for query stopping
        // Prejoin
        ArrayList<String> toJoinIds = new ArrayList<>();

        try {
            for (Pair<DataSource, OperatorChain> pair : preJoin) {
                DataSource ds = pair.getLeft();
                OperatorChain ops = pair.getRight();

                String prevId = ds.getName();
                if (!OpThreadManager.dataSourceOpThread.isInputExist(ds.getName())) {
                    OpThreadManager.dataSourceOpThread.addInput(prevId);
                }

                OpThread prevOpThread = OpThreadManager.dataSourceOpThread;
                Operator prevOp = ds;

                for (Operator op : ops.getOperators()) {
                    String inputId = UUID.randomUUID().toString();
                    SingleInputOpThread opThread = (SingleInputOpThread) OpThreadManager.getOpThread(op.getClass());
                    opThread.addInput(inputId);
                    SingleInputOpThread prevOpThreadX = (SingleInputOpThread) prevOpThread;

                    prevOpThreadX.addConnection(prevId, inputId, prevOp, op);

                    prevOpThread = opThread;
                    prevId = inputId;
                    prevOp = op;
                }

                String toJoinId = UUID.randomUUID().toString();
                toJoinIds.add(toJoinId);
                SingleInputOpThread prevOpThreadX = (SingleInputOpThread) prevOpThread;

                prevOpThreadX.addConnection(prevId, toJoinId, prevOp, joinOp);

            }


            // Join
            OpThreadManager.joinOpThread.addInput(toJoinIds);

            // Post-join
            String prevId = null;
            Operator prevOp = null;
            OpThread prevOpThread = OpThreadManager.joinOpThread;
            for (Operator op : this.postJoin.getOperators()) {
                String inputId = UUID.randomUUID().toString();
                SingleInputOpThread opThread = (SingleInputOpThread) OpThreadManager.getOpThread(op.getClass());
                opThread.addInput(inputId);
                if (prevOpThread.getClass() == JoinOpThread.class) {
                    JoinOpThread prevOpThreadX = (JoinOpThread) prevOpThread;
                    prevOpThreadX.addConnection(toJoinIds, inputId, joinOp, op);
                } else {
                    SingleInputOpThread prevOpThreadX = (SingleInputOpThread) prevOpThread;
                    prevOpThreadX.addConnection(prevId, inputId, prevOp, op);
                }


                prevOpThread = opThread;
                prevId = inputId;
                prevOp = op;
            }

            if (prevOpThread.getClass() == JoinOpThread.class) {
                String lastId = UUID.randomUUID().toString();
                OpThreadManager.outputOpThread.addInput(lastId, outputOp);
                JoinOpThread prevOpThreadX = (JoinOpThread) prevOpThread;
                prevOpThreadX.addConnection(toJoinIds, lastId, joinOp, outputOp);
            } else {
                String lastId = UUID.randomUUID().toString();
                OpThreadManager.outputOpThread.addInput(lastId, outputOp);
                SingleInputOpThread prevOpThreadX = (SingleInputOpThread) prevOpThread;
                prevOpThreadX.addConnection(prevId, lastId, prevOp, outputOp);
            }

            LOGGER.info("Provisioned query: " + this.id);
        } catch(Exception e) {
            e.printStackTrace();
        }
        return true;
    }

    public SubQuery(JSONObject query) throws NoSuchMethodException {
        super(query);
        if(hasExternalQueryDataSourceDefinedCorrectly()) {
            adjustArgsFieldNaming();
        }

        return;
    }

    private boolean hasExternalQueryDataSourceDefinedCorrectly() {
        boolean result = true;
        for(Pair<DataSource, OperatorChain> pair: preJoin) {
            if(pair.getLeft() instanceof ExternalQueryDataSource) {
                ExternalQueryDataSource ds = (ExternalQueryDataSource) pair.getLeft();
                if(ds.getSourceColumnName() == null) {
                    result = false;
                }
            }
        }

        return result;
    }

    // Field naming can be only occuring if columnNameMapping have been defined correctly for external query data source
    public void adjustArgsFieldNaming() {
        if(hasExternalQueryDataSourceDefinedCorrectly()) {
            for(Pair<DataSource, OperatorChain> pair: preJoin) {
                pair.getRight().adjustArgsFieldNaming(columnNameMapping);
            }

            postJoin.adjustArgsFieldNaming(columnNameMapping);
        }
    }

    public void updateExternalQueryMapping(String sourceQe, HashMap<String,String> mapping) {
        for(Pair<DataSource, OperatorChain> pair: preJoin) {
            if(pair.getLeft() instanceof ExternalQueryDataSource) {
                ExternalQueryDataSource ds = (ExternalQueryDataSource) pair.getLeft();
                if(ds.getSourceQe() == sourceQe) {
                    if(mapping.containsKey(ds.getColumnName())) {
                        ds.setSourceColumnName( mapping.get(ds.getColumnName()));
                        this.addColumnNameMapping(ds.getColumnName(), mapping.get(ds.getColumnName()));
                    }
                }
            }
        }
        this.adjustArgsFieldNaming();
    }

}
