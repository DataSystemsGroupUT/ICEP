package ee.ut.cs.dsg.d2ia.condition;

public class RelativeConditionNew extends RelativeCondition{

    private Expression internalRelativeExpression;

    public RelativeConditionNew(Expression internalExpression, Expression internalRelativeExpression) {
        super(internalExpression);
        this.internalRelativeExpression=internalRelativeExpression;
    }

    @Override
    public Expression getInternalExpression() {
        return internalRelativeExpression;
    }

    @Override
    public AbsoluteCondition getStartCondition() {
        return new AbsoluteConditionNew(super.getInternalExpression());
    }
}
