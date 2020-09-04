package ee.ut.cs.dsg.d2ia.condition;

public class Operation implements Expression{

    private Operation firstElem;
    private Operation second;
    private Operator operator;

    protected Operation() {
    }

    public Operation(Operation first, Operator operator, Operation second) {
        this.firstElem = first;
        this.second = second;
        this.operator = operator;
    }

    public Operation(Operator operator, Operation second) {
        this.second = second;
        this.operator = operator;
    }

    @Override
    public Operation first() {
        return firstElem;
    }

    @Override
    public Operation second() {
        return second;
    }

    @Override
    public boolean eval(double first, double last, double min, double max, double sum, int count, double currentValue, double avg) {
        throw new UnsupportedOperationException("Operation, does not return a boolean value.");
    }


    @Override
    public double calculate(double first, double last, double min, double max, double sum, int count, double currentValue, double avg) {
        switch(operator){
            case Plus: return firstElem.calculate(first, last, min, max, sum, count, currentValue, avg) + second.calculate(first, last, min, max, sum, count, currentValue, avg);
            case Minus: return firstElem.calculate(first, last, min, max, sum, count, currentValue,avg)-second.calculate(first, last, min, max, sum, count, currentValue, avg);
            case Multiply: return firstElem.calculate(first, last, min, max, sum, count, currentValue, avg)*second.calculate(first, last, min, max, sum, count, currentValue, avg);
            case Divide: return  firstElem.calculate(first, last, min, max, sum, count, currentValue, avg)/second.calculate(first, last, min, max, sum, count, currentValue, avg);
            case Absolute: return  Math.abs(second.calculate(first, last, min, max, sum, count, currentValue, avg));
            default: throw new IllegalArgumentException();
        }
    }

}
