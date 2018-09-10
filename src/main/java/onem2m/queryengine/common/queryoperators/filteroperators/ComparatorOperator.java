package onem2m.queryengine.common.queryoperators.filteroperators;

public interface ComparatorOperator {
    public String getSyntax();
    boolean operate(Object left, Object right, boolean isNumber);
}
