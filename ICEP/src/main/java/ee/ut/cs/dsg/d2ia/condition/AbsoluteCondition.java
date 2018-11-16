package ee.ut.cs.dsg.d2ia.condition;

public class AbsoluteCondition extends Condition {


    @Override
    public Condition LHS(Object operand) {
        this.lhs = operand;
        return this;
    }

    @Override
    public Condition RHS(Object operand) {
        this.rhs = operand;
        return this;
    }

    @Override
    public Condition operator(Operator op) {
        this.operator = op;
        return this;
    }

    public String parse(double first, double last, double min, double max, double sum, int count, double currentValue)
    {
        String conditionStringLHS = "";
        String conditionStringOperator = "";
        String conditionStringRHS = "";
        if (lhs == null)
            conditionStringLHS="";
        else if (lhs instanceof Operand)
        {
            Operand lhsOperand = (Operand) lhs;
            if (lhsOperand == Operand.Average) {
                conditionStringLHS += (sum / count);
            } else if (lhsOperand == Operand.Sum) {
                conditionStringLHS += sum;
            } else if (lhsOperand == Operand.First) {
                conditionStringLHS += first;
            } else if (lhsOperand == Operand.Last) {
                conditionStringLHS += last;
            } else if (lhsOperand == Operand.Max) {
                conditionStringLHS += max;
            } else if (lhsOperand == Operand.Min) {
                conditionStringLHS += min;
            } else if (lhsOperand == Operand.Value) {
                conditionStringLHS += currentValue;
            }
        }
        else if (lhs instanceof Boolean)
            conditionStringLHS += "true";
        else if (lhs instanceof Double)
            conditionStringLHS += lhs.toString();
        else if (lhs instanceof  Integer || lhs instanceof Long)
            conditionStringLHS += lhs.toString();
        else if (lhs instanceof AbsoluteCondition)
            conditionStringLHS += "("+((AbsoluteCondition) lhs).parse(first, last, min, max, sum, count, currentValue)+")";

        if (operator != null)
            conditionStringOperator+=" "+operator.toString();

        if (rhs instanceof Operand)
        {
            Operand rhsOperand = (Operand) rhs;
            if (rhsOperand == Operand.Average) {
                conditionStringRHS += (sum / count);
            } else if (rhsOperand == Operand.Sum) {
                conditionStringRHS += sum;
            } else if (rhsOperand == Operand.First) {
                conditionStringRHS += first;
            } else if (rhsOperand == Operand.Last) {
                conditionStringRHS += last;
            } else if (rhsOperand == Operand.Max) {
                conditionStringRHS += max;
            } else if (rhsOperand == Operand.Min) {
                conditionStringRHS += min;
            } else if (rhsOperand == Operand.Value) {
                conditionStringRHS += currentValue;
            }
        }
        else if (rhs instanceof Boolean)
            conditionStringRHS += "true";
        else if (rhs instanceof Double)
            conditionStringRHS += rhs.toString();
        else if (rhs instanceof  Long || rhs instanceof Integer)
            conditionStringRHS += rhs.toString();
        else if (rhs instanceof AbsoluteCondition)
            conditionStringRHS += "("+((AbsoluteCondition) rhs).parse(first, last, min, max, sum, count, currentValue)+")";

        if (conditionStringOperator.contains("$$$"))
            return conditionStringOperator.replace("$$$", conditionStringRHS);
        else
            return conditionStringLHS+ " "+conditionStringOperator + conditionStringRHS;
    }

}
