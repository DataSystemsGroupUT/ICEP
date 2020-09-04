package ee.ut.cs.dsg.d2ia.condition;

public class ConditionNew implements Expression {

    private Operator operator;
    private Expression firstElem;
    private Expression second;

    public ConditionNew(Expression first, Operator operator, Expression second) {
        this.operator = operator;
        this.firstElem = first;
        this.second = second;
    }

    public ConditionNew() {
    }

    @Override
    public Expression first() {
        return firstElem;
    }

    @Override
    public Expression second() {
        return second;
    }

    public boolean eval(double first, double last, double min, double max, double sum, int count, double currentValue, double avg){
        switch(operator){
            case Equals: return firstElem.calculate(first, last, min, max, sum, count, currentValue, avg)==second.calculate(first, last, min, max, sum, count, currentValue, avg);
            case LessThan: return firstElem.calculate(first, last, min, max, sum, count, currentValue, avg)<second.calculate(first, last, min, max, sum, count, currentValue, avg);
            case LessThanEqual: return firstElem.calculate(first, last, min, max, sum, count, currentValue, avg)<=second.calculate(first, last, min, max, sum, count, currentValue, avg);
            case GreaterThan: return firstElem.calculate(first, last, min, max, sum, count, currentValue, avg)>second.calculate(first, last, min, max, sum, count, currentValue, avg);
            case GreaterThanEqual: return firstElem.calculate(first, last, min, max, sum, count, currentValue, avg)>=second.calculate(first, last, min, max, sum, count, currentValue, avg);
            case NotEqual: return firstElem.calculate(first, last, min, max, sum, count, currentValue, avg)!=second.calculate(first, last, min, max, sum, count, currentValue, avg);
            case Not: return !second.eval(first, last, min, max, sum, count, currentValue, avg);
            case And: return firstElem.eval(first, last, min, max, sum, count, currentValue, avg) && second.eval(first, last, min, max, sum, count, currentValue, avg);
            case Or: return  firstElem.eval(first, last, min, max, sum, count, currentValue, avg) || second.eval(first, last, min, max, sum, count, currentValue, avg);
            default: throw new IllegalArgumentException();
        }
    }

    @Override
    public double calculate(double first, double last, double min, double max, double sum, int count, double currentValue, double avg) {
        throw new UnsupportedOperationException("Boolean expression, does not return a numerical value.");
    }
}
