package onem2m.queryengine.common.queryoperators.filteroperators;

public class And implements FilterOperator {
    @Override
    public boolean operate(boolean current, boolean conditionBool) {
        return current && conditionBool;
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof And) {
            return true;
        }

        return false;
    }

    @Override
    public String toString() {
        return "&";
    }
}
