package ee.ut.cs.dsg.icep.condition;

public enum Operator
{
    Equals ,
    LessThan,
    LessThanEqual,
    GreaterThan,
    GreaterThanEqual,
    NotEqual;

    @Override
    public String toString()
    {
        switch(this){
            case Equals: return "==";
            case LessThan: return "<";
            case LessThanEqual: return "<=";
            case GreaterThan: return ">";
            case GreaterThanEqual: return ">=";
            case NotEqual: return "!=";
            default: throw new IllegalArgumentException();
        }
    }
}