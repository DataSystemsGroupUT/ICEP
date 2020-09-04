package ee.ut.cs.dsg.d2ia.condition;

import ee.ut.cs.dsg.d2ia.event.IntervalStatistics;
import ee.ut.cs.dsg.d2ia.event.RawEvent;
import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.ExpressionEvaluator;
import scala.tools.partest.ScaladocModelTest$access$WithMembers$class;

import javax.script.ScriptException;
import javax.sound.midi.SoundbankResource;
import java.lang.reflect.InvocationTargetException;
import java.util.Random;

public class JaninoConditionEvaluator<S extends RawEvent> extends ConditionEvaluator<S>{

    private Random random;
    private ExpressionEvaluator expressionEvaluator;

    public JaninoConditionEvaluator() {
        this.random = new Random();
    }

    @Override
    public boolean evaluateCondition(AbsoluteCondition condition, S s) throws Exception {
        if(expressionEvaluator == null){
            expressionEvaluator = new ExpressionEvaluator();
            String[] parameters = new String[]{"first","last","min","max","sum","count", "currentValue", "avg"};
            Class<?>[] parametersClass = new Class[]{double.class, double.class, double.class, double.class, double.class,int.class, double.class, double.class};
            String conditionString = condition.parse("first","last","min","max","sum","count", "currentValue", "avg");
            expressionEvaluator.setExpressionType(boolean.class);
            expressionEvaluator.setParameters(parameters, parametersClass);
            expressionEvaluator.cook(conditionString);
        }

        return (boolean) expressionEvaluator.evaluate(new Object[]{0,0,0,0,0,0,s.getValue(),0});


    }

    @Override
    public boolean evaluateRelativeCondition(RelativeCondition condition, IntervalStatistics stats, S s) throws Exception {
        first = stats.first;
        last = stats.last;
        min = stats.min;
        max = stats.max;
        sum = stats.sum;
        count = stats.count;

        return evaluateRelativeConditionInternal(condition, s);

    }

    @Override
    public boolean evaluateRelativeCondition(RelativeCondition condition, Iterable<S> prevMatches, S s) throws Exception {
        //      System.out.println("Current event "+s.toString());
        // update intermediate results
        if (prevMatches != null && prevMatches.iterator().hasNext() ) {
            resetStats();
            for (S ss : prevMatches) //Iterables preserve order
            {
                //               System.out.println("Previous item "+ss.toString() + " of current event "+s.toString());
                sum += ss.getValue();
                count++;
                min = Double.min(min, ss.getValue());
                max = Double.max(max, ss.getValue());
                if (first == Double.MIN_VALUE)
                    first = ss.getValue();

                last = ss.getValue();
            }
            // we have to add the current element

        }
        else
        {
            if (first == Double.MIN_VALUE) {
                first = s.getValue();
                last = s.getValue();
                min = s.getValue();
                max = s.getValue();
            }
        }
        sum += s.getValue();
        count++;


        return evaluateRelativeConditionInternal(condition, s);
    }

    @Override
    protected boolean evaluateRelativeConditionInternal(RelativeCondition condition, S s) throws ScriptException {
//        ScriptEngineManager mgr = new ScriptEngineManager();
//        ScriptEngine engine = mgr.getEngineByName("JavaScript");

        if(expressionEvaluator==null){
            try {
                initializeEval(condition);
            } catch (CompileException e) {
                e.printStackTrace();
            }
        }


        boolean accepted=false;
        try {
            accepted = (boolean) expressionEvaluator.evaluate(new Object[]{first, last, min, max, sum, count, s.getValue(),sum/count});
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }
        min = Math.min(min, s.getValue());
        max = Math.max(max, s.getValue());
        last = s.getValue();
        return accepted;
    }

    public void initializeEval(RelativeCondition condition) throws CompileException {

        String conditionString;// = condition.toString().split("Relative")[1].trim();

        Object relativeLHS = condition.getRelativeLHS();
        Object relativeRHS = condition.getRelativeRHS();
        Operator relativeOperator = condition.getRelativeOperator();
        double relativeLHSDouble = Double.MIN_VALUE, relativeRHSDouble = Double.MIN_VALUE;
        Operand relativeLHSOperand = null, relativeRHSOperand = null;


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


//        if ((relativeRHSOperand != null && relativeRHSOperand != Operand.Constant)
//                || (relativeLHSOperand != null && relativeLHSOperand != Operand.Constant)) {


//        }
        String lhsConditionString = null;
        String rhsConditionString = null;
        if (relativeLHS instanceof AbsoluteCondition) {
            lhsConditionString = ((AbsoluteCondition) relativeLHS).parse("first","last","min","max","sum","count", "currentValue", "avg");
        }
        if (relativeRHS instanceof AbsoluteCondition) {
            rhsConditionString = ((AbsoluteCondition) relativeRHS).parse("first","last","min","max","sum","count", "currentValue", "avg");
        }
        // now we can evaluate the condition
        conditionString = "";
        conditionString = getOperandString(conditionString, relativeLHSDouble, relativeLHSOperand, lhsConditionString);

        conditionString += " " + relativeOperator.toString() +"(";

        conditionString = getOperandString(conditionString, relativeRHSDouble, relativeRHSOperand, rhsConditionString);
        conditionString+=")";

        System.out.println(conditionString);

        expressionEvaluator = new ExpressionEvaluator();
        String[] parameters = new String[]{"first","last","min","max","sum","count", "currentValue", "avg"};
        Class<?>[] parametersClass = new Class[]{double.class, double.class, double.class, double.class, double.class,int.class, double.class, double.class};
        expressionEvaluator.setExpressionType(boolean.class);
        expressionEvaluator.setParameters(parameters, parametersClass);
        expressionEvaluator.cook(conditionString);
    }

    private String getOperandString(String conditionString, double relativeRHSDouble, Operand relativeRHSOperand, String rhsConditionString) {
        if (relativeRHSDouble != Double.MIN_VALUE) {
            conditionString += relativeRHSDouble;
        } else if (relativeRHSOperand != null) {
            if (relativeRHSOperand == Operand.Average) {
                conditionString += "avg";
            } else if (relativeRHSOperand == Operand.Sum) {
                conditionString += "sum";
            } else if (relativeRHSOperand == Operand.First) {
                conditionString += "first";
            } else if (relativeRHSOperand == Operand.Last) {
                conditionString += "last";
            } else if (relativeRHSOperand == Operand.Max) {
                conditionString += "max";
            } else if (relativeRHSOperand == Operand.Min) {
                conditionString += "min";
            } else if (relativeRHSOperand == Operand.Value) {
                conditionString += "currentValue";
            }

        } else
            conditionString += rhsConditionString;
        return conditionString;
    }
}
