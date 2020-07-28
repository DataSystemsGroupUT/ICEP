package ee.ut.cs.dsg.d2ia.condition;

import ee.ut.cs.dsg.d2ia.event.IntervalStatistics;
import ee.ut.cs.dsg.d2ia.event.RawEvent;

import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

public class CustomConditionEvaluator<S extends RawEvent> extends ConditionEvaluator<S>{

    private boolean initialize;

    public CustomConditionEvaluator() {
        super();
        initialize = false;
    }

    @Override
    public boolean evaluateCondition(AbsoluteCondition condition, S s) throws Exception {
        return condition.getInternalExpression().eval(0,0,0,0,0,0,s.getValue(), 0);
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
        boolean result = condition.getInternalExpression().eval(first, last, min, max, sum, count, s.getValue(), sum/count);

        min = Math.min(min, s.getValue());
        max = Math.max(max, s.getValue());
        last = s.getValue();
        return result;
    }
}
