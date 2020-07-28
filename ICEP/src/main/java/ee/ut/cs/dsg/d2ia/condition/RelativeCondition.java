package ee.ut.cs.dsg.d2ia.condition;

public class RelativeCondition extends Condition {
    private Object relativeLHS;
    private Object relativeRHS;
    private Operator relativeOperator;

    @Override
    public RelativeCondition LHS(Object operand) {
        this.lhs = operand;
        return this;
    }

    public RelativeCondition() {
    }

    public RelativeCondition(Expression internalExpression) {
        super(internalExpression);
    }

    @Override
    public RelativeCondition RHS(Object operand) {
        this.rhs = operand;
        return this;
    }

    @Override
    public RelativeCondition operator(Operator op) {
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
        if (!(relativeLHS instanceof Operand) && !(relativeLHS instanceof AbsoluteCondition))
            return relativeLHS.toString();
        return relativeLHS;
    }

    public Object getRelativeRHS()
    {
        if (!(relativeRHS instanceof Operand) && !(relativeRHS instanceof AbsoluteCondition))
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
    public AbsoluteCondition getStartCondition()
    {
        AbsoluteCondition absoluteCondition = new AbsoluteCondition();
        absoluteCondition.LHS(this.lhs).RHS(this.rhs).operator(this.operator);
        return  absoluteCondition;
    }
}
