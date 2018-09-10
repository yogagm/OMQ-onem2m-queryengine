package onem2m.queryengine.common.queryoperators.filteroperators;

import org.json.simple.JSONObject;

import java.util.HashMap;

public class Condition {
    private String columnName;
    private ComparatorOperator comparator;
    private Object toCompare;
    private boolean toCompareIsConstant = true;
    private boolean toCompareIsNumber = true;

    public Condition(String predicateStr) {
        ComparatorOperator[] comparatorList = new ComparatorOperator[]{
                new Equal(),
                new Inequal(),
                new LargerThanEqual(),
                new SmallerThanEqual(),
                new LargerThan(),
                new SmallerThan()
        };

        for (ComparatorOperator comparator : comparatorList) {
            String comparatorSyntax = comparator.getSyntax();
            String[] equal = predicateStr.split(comparatorSyntax);
            if (equal.length == 2) {
                this.columnName = equal[0];
                this.comparator = comparator;

                String[] toCompareArr = equal[1].split(":");
                String toCompare = null;
                if (toCompareArr.length == 2) {
                    toCompare = toCompareArr[1];
                    if (toCompareArr[0].equals("column")) {
                        this.toCompareIsConstant = false;
                    }
                } else {
                    toCompare = equal[1];
                }

                if (toCompare.matches("[0-9.]*")) {
                    this.toCompare = Double.parseDouble(toCompare);
                } else {
                    this.toCompare = toCompare;
                    this.toCompareIsNumber = false;
                }

                break;
            }
        }
    }

    public boolean operate(JSONObject input) {
        Object left = null, right = null;
        if(this.toCompareIsConstant == true) {
            left = input.get(this.columnName);
            right = this.toCompare;
        } else if(this.toCompareIsConstant == false) {
            left = input.get(this.columnName);
            right = input.get(this.toCompare);
        }


        return comparator.operate(left, right, this.toCompareIsNumber);
    }

    public String getColumnName() {
        return columnName;
    }

    @Override
    public boolean equals(Object obj) {
        Condition that = (Condition) obj;
        boolean equal = true;

        if(!this.columnName.equals(that.columnName)) {
            equal = false;
        }

        if(!this.comparator.equals(that.comparator)) {
            equal = false;
        }

        if(!this.toCompare.equals(that.toCompare)) {
            equal = false;
        }

        return equal;
    }

    @Override
    public String toString() {
        if(this.toCompareIsConstant) {
            return this.columnName + this.comparator.getSyntax() + this.toCompare;
        } else {
            return this.columnName + this.comparator.getSyntax() + "column:" + this.toCompare;
        }
    }

    public void adjustArgsFieldNaming(HashMap<String, String> columnNameMapping) {
        // [NEED TESTING] TODO: (json-data-support) only detect the before "." part of columnName and toCompare
        String[] columnNameSplit = columnName.split("\\.", 2);
        String columnNameToFind = columnNameSplit[0];
        if(columnNameMapping.containsKey(columnNameToFind)) {
            columnName = columnNameMapping.get(columnNameToFind);
            if(columnNameSplit.length > 1) {
                columnName += "." + columnNameSplit[1];
            }
        }


        if(!toCompareIsConstant) {
            String toCompareX = (String) toCompare;
            String[] toCompareSplit = toCompareX.split("\\.", 2);
            String toCompareToFind = toCompareSplit[0];
            if(columnNameMapping.containsKey(toCompareToFind)) {
                toCompare = columnNameMapping.get(toCompareToFind);
                if(toCompareSplit.length > 1) {
                    toCompare += "." + toCompareSplit[1];
                }
            }
        }
    }
}
