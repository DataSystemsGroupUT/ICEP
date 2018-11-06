package ee.ut.cs.dsg.d2ia.generator;


import ee.ut.cs.dsg.d2ia.condition.Operand;
import ee.ut.cs.dsg.d2ia.event.IntervalEvent;
import ee.ut.cs.dsg.d2ia.event.RawEvent;
import org.apache.flink.cep.PatternSelectFunction;

import java.util.List;
import java.util.Map;

public class HomogeneousIntervalElementsCollector<S extends RawEvent, W extends IntervalEvent> implements PatternSelectFunction<S, W> {

    private Class<W> out;
    private String outValueDescription;
    private Operand outValueOperand;

    public HomogeneousIntervalElementsCollector(Class<W> out, Operand outputValueOperand) {
        this.out = out;
        this.outValueOperand = outputValueOperand;
    }

    @Override
    public W select(Map<String, List<S>> map) throws Exception {
        List<S> matchingEvents = map.get("1");
        double outputValue = 0;
        String rid = matchingEvents.get(0).getKey();
        outValueDescription = outValueOperand.toString();
        if (outValueOperand == Operand.First) {
            outputValue = matchingEvents.get(0).getValue();

        } else if (outValueOperand == Operand.Last) {
            outputValue = matchingEvents.get(matchingEvents.size() - 1).getValue();
        } else         if (outValueOperand == Operand.Average) {
            for (S s : matchingEvents) {
                outputValue += s.getValue();
            }
            outputValue = outputValue / matchingEvents.size();
        } else         if (outValueOperand == Operand.Sum) {
            for (S s : matchingEvents) {
                outputValue += s.getValue();
            }
        } else if (outValueOperand == Operand.Max ){
            outputValue = matchingEvents.get(0).getValue();
            for (S s:matchingEvents)
            {
                outputValue = Double.max(outputValue, s.getValue());
            }
        } else if (outValueOperand == Operand.Min ) {
            outputValue = matchingEvents.get(0).getValue();
            for (S s : matchingEvents) {
                outputValue = Double.min(outputValue, s.getValue());
            }
        }
        long start, end;
        start = matchingEvents.get(0).getTimestamp();
        end = matchingEvents.get(matchingEvents.size()-1).getTimestamp();

        W element = out.getDeclaredConstructor( long.class, long.class, double.class, String.class, String.class).newInstance(start, end, outputValue, outValueDescription, rid);
        return element;

    }
}
