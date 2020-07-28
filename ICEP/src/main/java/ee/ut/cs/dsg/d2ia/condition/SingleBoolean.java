package ee.ut.cs.dsg.d2ia.condition;

public class SingleBoolean extends ConditionNew{
    private boolean booleanValue;

    public SingleBoolean(boolean booleanValue) {
        this.booleanValue = booleanValue;
    }

    @Override
    public Expression first() {
        throw new UnsupportedOperationException("This is a simple boolean wrapper, does not have the first member.");
    }

    @Override
    public ConditionNew second() {
        return this;
    }

    @Override
    public boolean eval(double first, double last, double min, double max, double sum, int count, double currentValue, double avg) {
        return booleanValue;
    }
}
