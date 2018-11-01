package ee.ut.cs.dsg.icep.condition;

public class AbsoluteCondition extends Condition {


    @Override
    protected Condition LHS(Object operand) {
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



}
