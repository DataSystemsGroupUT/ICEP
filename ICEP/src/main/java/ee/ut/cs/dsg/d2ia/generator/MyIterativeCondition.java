package ee.ut.cs.dsg.d2ia.generator;

import ee.ut.cs.dsg.d2ia.condition.*;
import ee.ut.cs.dsg.d2ia.event.RawEvent;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;


import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

public class MyIterativeCondition<S extends RawEvent> extends IterativeCondition<S> {

    public enum ConditionContainer
    {
        Until,
        Where
    }
    private ConditionContainer container;
    private static final long serialVersionUID = 2392863109523984059L;

    private Condition condition;
    public MyIterativeCondition(Condition cond, ConditionContainer container) {
        condition = cond; this.container = container;
    }
    private boolean evaluateCondition(Condition condition, S s) throws Exception {
        ScriptEngineManager mgr = new ScriptEngineManager();
        ScriptEngine engine = mgr.getEngineByName("JavaScript");
        String conditionString = condition.toString();

        if (condition instanceof RelativeCondition) {
            conditionString = conditionString.split("Relative")[0].trim();

        }
        conditionString = conditionString.replace("value", Double.toString(s.getValue()));
        boolean result = (boolean) engine.eval(conditionString);
        if (container == ConditionContainer.Until)
            return !result;
        else
            return result;
    }

    //TODO: Try the until condition
    private boolean evaluateRelativeCondition(RelativeCondition condition, Iterable<S> prevMatches, S s) throws Exception {
        ScriptEngineManager mgr = new ScriptEngineManager();
        ScriptEngine engine = mgr.getEngineByName("JavaScript");
        String conditionString;// = condition.toString().split("Relative")[1].trim();

        Object relativeLHS = condition.getRelativeLHS();
        Object relativeRHS = condition.getRelativeRHS();
        Operator relativeOperator = condition.getRelativeOperator();
        double relativeLHSDouble = Double.MIN_VALUE, relativeRHSDouble = Double.MIN_VALUE;
        Operand relativeLHSOperand = null, relativeRHSOperand = null;
        double sum = 0;
        int count = 0;
        double min = Double.MAX_VALUE;
        double max = Double.MIN_VALUE;
        double first = Double.MIN_VALUE;
        double last = -1;

        if (relativeLHS instanceof String) {
            relativeLHSDouble = Double.parseDouble(((String) relativeLHS));
        }

        if (relativeRHS instanceof String) {
            relativeRHSDouble = Double.parseDouble(((String) relativeRHS));
        }

        if (relativeLHS instanceof Operand) {
            relativeLHSOperand = (Operand) relativeLHS;
        }

        if (relativeRHS instanceof Operand) {
            relativeRHSOperand = (Operand) relativeRHS;
        }

        if ((relativeRHSOperand != null && relativeRHSOperand != Operand.Constant)
                || (relativeLHSOperand != null && relativeLHSOperand != Operand.Constant)) {

            for (S ss : prevMatches) //Iterables preserve order
            {
                sum += ss.getValue();
                count++;
                min = Double.min(min, ss.getValue());
                max = Double.max(max, ss.getValue());
                if (first == Double.MIN_VALUE)
                    first = ss.getValue();

                last = ss.getValue();
            }


        }
        // now we can evaluate the condition
        conditionString = "";
        if (relativeLHSDouble != Double.MIN_VALUE) {
            conditionString += relativeLHSDouble;
        } else if (relativeLHSOperand != null) {
            if (relativeLHSOperand == Operand.Average) {
                conditionString += (sum / count);
            } else if (relativeLHSOperand == Operand.Sum) {
                conditionString += sum;
            } else if (relativeLHSOperand == Operand.First) {
                conditionString += first;
            } else if (relativeLHSOperand == Operand.Last) {
                conditionString += last;
            } else if (relativeLHSOperand == Operand.Max) {
                conditionString += max;
            } else if (relativeLHSOperand == Operand.Min) {
                conditionString += min;
            } else if (relativeLHSOperand == Operand.Value) {
                conditionString += s.getValue();
            }

        }

        conditionString += " " + relativeOperator.toString();

        if (relativeRHSDouble != Double.MIN_VALUE) {
            conditionString += relativeRHSDouble;
        } else if (relativeRHSOperand != null) {
            if (relativeRHSOperand == Operand.Average) {
                conditionString += (sum / count);
            } else if (relativeRHSOperand == Operand.Sum) {
                conditionString += sum;
            } else if (relativeRHSOperand == Operand.First) {
                conditionString += first;
            } else if (relativeRHSOperand == Operand.Last) {
                conditionString += last;
            } else if (relativeRHSOperand == Operand.Max) {
                conditionString += max;
            } else if (relativeRHSOperand == Operand.Min) {
                conditionString += min;
            } else if (relativeRHSOperand == Operand.Value) {
                conditionString += s.getValue();
            }

        }
        boolean result = (boolean) engine.eval(conditionString);

        if (container == ConditionContainer.Until)
            return !result;
        else
            return result;
    }

    @Override
    public boolean filter(S s, Context<S> context) throws Exception {


        if (condition instanceof AbsoluteCondition)
            return evaluateCondition(condition, s);
        else {
            Iterable<S> items = context.getEventsForPattern("1");
            if (!items.iterator().hasNext()) {
                return evaluateCondition(condition, s);
            } else // there are previous items
            {
                return evaluateRelativeCondition((RelativeCondition) condition, items, s);
            }
        }


    }
}

