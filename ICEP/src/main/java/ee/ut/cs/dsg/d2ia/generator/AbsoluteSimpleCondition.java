package ee.ut.cs.dsg.d2ia.generator;

import ee.ut.cs.dsg.d2ia.condition.AbsoluteCondition;
import ee.ut.cs.dsg.d2ia.condition.ConditionEvaluator;
import ee.ut.cs.dsg.d2ia.event.RawEvent;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;

public class AbsoluteSimpleCondition<S extends RawEvent> extends SimpleCondition<S> {

    AbsoluteCondition condition;
    ConditionEvaluator conditionEvaluator;
    public AbsoluteSimpleCondition(AbsoluteCondition condition)
    {
        this.condition = condition;
        conditionEvaluator = new ConditionEvaluator<>();
    }
    @Override
    public boolean filter(S s) throws Exception {
        return conditionEvaluator.evaluateCondition(condition, s);
    }
}
