package ee.ut.cs.dsg.d2ia.condition;

public enum Operator
{
    Equals ,
    LessThan,
    LessThanEqual,
    GreaterThan,
    GreaterThanEqual,
    NotEqual,
    Plus,
    Minus,
    Multiply,
    Divide,
    Not,
    And,
    Or,
    Absolute;

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
            case Plus: return "+";
            case Minus: return "-";
            case Multiply: return "*";
            case Divide: return  "/";
            case Not: return "!";
            case And: return "&&";
            case Or: return  "||";
            case Absolute: return "Math.abs($$$)";
            default: throw new IllegalArgumentException();
        }
    }
}