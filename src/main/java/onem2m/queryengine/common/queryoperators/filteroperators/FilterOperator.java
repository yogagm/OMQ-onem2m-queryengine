package onem2m.queryengine.common.queryoperators.filteroperators;

public interface FilterOperator {
    boolean operate(boolean current, boolean conditionBool);

}
