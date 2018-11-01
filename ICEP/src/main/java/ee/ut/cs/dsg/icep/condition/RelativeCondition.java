package ee.ut.cs.dsg.icep.condition;

public class RelativeCondition extends Condition {
    private Object relativeLHS;
    private Object relativeRHS;
    private Operator relativeOperator;

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

    public RelativeCondition relativeLHS(Object operand)  {
        this.relativeLHS = operand;
        return this;
    }
    public RelativeCondition relativeRHS(Object operand)  {
        this.relativeRHS = operand;
        return this;
    }
    public RelativeCondition relativeOperator(Operator op)  {
        this.relativeOperator = op;
        return this;
    }

    public Object getRelativeLHS()
    {
        if (!(relativeLHS instanceof Operand))
            return relativeLHS.toString();
        return relativeLHS;
    }

    public Object getRelativeRHS()
    {
        if (!(relativeRHS instanceof Operand))
            return relativeRHS.toString();
        return relativeRHS;
    }

    public Operator getRelativeOperator()
    {
        return relativeOperator;
    }

    public String toString()
    {
        return super.toString() + " Relative " + relativeLHS.toString()+relativeOperator.toString()+relativeRHS.toString();
    }
}
