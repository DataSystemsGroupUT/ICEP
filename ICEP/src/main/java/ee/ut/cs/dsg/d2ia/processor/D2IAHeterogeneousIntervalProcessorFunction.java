package ee.ut.cs.dsg.d2ia.processor;

import ee.ut.cs.dsg.d2ia.event.IntervalEvent;
import ee.ut.cs.dsg.d2ia.event.RawEvent;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

public class D2IAHeterogeneousIntervalProcessorFunction<S extends RawEvent, E extends RawEvent, F extends RawEvent, I extends IntervalEvent> extends ProcessWindowFunction<RawEvent, I, String, GlobalWindow> {
    @Override
    public void process(String s, Context context, Iterable<RawEvent> iterable, Collector<I> collector) throws Exception {

    }
}
