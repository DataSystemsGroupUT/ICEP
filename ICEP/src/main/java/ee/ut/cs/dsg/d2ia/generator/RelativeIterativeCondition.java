package ee.ut.cs.dsg.d2ia.generator;

import ee.ut.cs.dsg.d2ia.condition.*;
import ee.ut.cs.dsg.d2ia.event.RawEvent;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;

public class RelativeIterativeCondition<S extends RawEvent> extends IterativeCondition<S> {

    public enum ConditionContainer {
        Until,
        Where
    }

    private ConditionContainer container;
    private static final long serialVersionUID = 2392863109523984059L;
    //private boolean intervalEntryMatched = false;
    private Condition condition;
    private ConditionEvaluator<S> conditionEvaluator;
    public RelativeIterativeCondition(Condition cond, ConditionContainer container) {
        conditionEvaluator = new ConditionEvaluator<>();
        condition = cond;
        this.container = container;
    }

    private boolean evaluateCondition(AbsoluteCondition condition, S s) throws Exception {

        boolean result = conditionEvaluator.evaluateCondition(condition, s);
//        if (result == true) // we can start an interval entry match
//            intervalEntryMatched = true;

        if (container == ConditionContainer.Until) {

            return !result;
        } else
            return result;
    }


    private boolean evaluateRelativeCondition(RelativeCondition condition, Iterable<S> prevMatches, S s) throws Exception {

        boolean result = conditionEvaluator.evaluateRelativeCondition(condition,prevMatches, s);
//        if (result == false)
//            intervalEntryMatched = false;
        if (container == ConditionContainer.Until)
            return !result;
        else
            return result;
    }

    @Override
    public boolean filter(S s, Context<S> context) throws Exception {


        if (condition instanceof AbsoluteCondition)
            return evaluateCondition((AbsoluteCondition) condition, s);
        else {

            Iterable<S> items = context.getEventsForPattern("1");
            if (!items.iterator().hasNext()) {
                return evaluateCondition(((RelativeCondition) condition).getStartCondition(), s);
            } else // there are previous items
            {
                return  evaluateRelativeCondition((RelativeCondition) condition, items, s);
//                boolean result
//                if (!intervalEntryMatched && (container == ConditionContainer.Until ? result: !result)) // this breaks a past interval and we need to check if it creates a new one
//                    return evaluateCondition(((RelativeCondition) condition).getStartCondition(), s);
//                else
//                    return result;
            }
        }


    }
}

