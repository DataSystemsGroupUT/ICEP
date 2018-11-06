package ee.ut.cs.dsg.d2ia.condition;

import java.io.Serializable;

public abstract class Condition implements Serializable {

    protected Object lhs;
    protected Object rhs;

    protected Operator operator;

    protected abstract Condition LHS(Object operand);

    public abstract Condition RHS(Object operand);

    public abstract Condition operator(Operator op);

    public String toString() {
        if (lhs == null)
        {
            lhs = Operand.Value;
        }
        return lhs.toString() + operator.toString() + rhs.toString();
    }


    public Object getRHS()
    {
        return rhs;
    }

    public Operator getOperator()
    {
        return operator;
    }
}
