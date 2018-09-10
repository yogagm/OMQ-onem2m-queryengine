package onem2m.queryengine.common.queryoperators.filteroperators;

public class Inequal implements ComparatorOperator {
    @Override
    public String getSyntax() {
        return "!=";
    }

    @Override
    public boolean operate(Object left, Object right, boolean isNumber) {
        if(isNumber) {
            return ((Double) left).doubleValue() != ((Double) right).doubleValue();
        } else {
            return !left.equals(right);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof Inequal) {
            return true;
        }

        return false;
    }
}
