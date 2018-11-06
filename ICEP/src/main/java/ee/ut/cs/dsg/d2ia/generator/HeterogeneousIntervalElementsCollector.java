package ee.ut.cs.dsg.d2ia.generator;

import ee.ut.cs.dsg.d2ia.event.IntervalEvent;
import ee.ut.cs.dsg.d2ia.event.RawEvent;
import org.apache.flink.cep.PatternSelectFunction;

import java.util.List;
import java.util.Map;



public class HeterogeneousIntervalElementsCollector<W extends IntervalEvent> implements PatternSelectFunction<RawEvent, W> {

    private Class<W> out;
    private String outValueDescription;


    public HeterogeneousIntervalElementsCollector(Class<W> out) {
        this.out = out;

    }

    @Override
    public W select(Map<String, List<RawEvent>> map) throws Exception {


        RawEvent startEvent, endEvent;

        startEvent = map.get("start").get(0);
        endEvent = map.get("end").get(0);


        long start, end;
        start = startEvent.getTimestamp();
        end = endEvent.getTimestamp();

        W element = out.getDeclaredConstructor( long.class, long.class, double.class, String.class, String.class).newInstance(start, end, 0, "", startEvent.getKey());
        return element;

    }
}
