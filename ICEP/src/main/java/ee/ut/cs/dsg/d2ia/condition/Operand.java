package ee.ut.cs.dsg.d2ia.condition;

public enum Operand {
    Average,
    Sum,
    Max,
    Min,
    First,
    Last,
    Constant,
    Value;

    @Override
    public String toString()
    {
        switch(this){
            case Average: return "avg";
            case Sum: return "sum";
            case Max: return "max";
            case Min: return "min";
            case First: return "first";
            case Last: return "last";
            case Constant: return "constant";
            case Value: return "value";
            default: throw new IllegalArgumentException();
        }
    }

}
