package ee.ut.cs.dsg.d2ia.evictor;

import ee.ut.cs.dsg.d2ia.event.RawEvent;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;

import java.util.Iterator;

public class WatermarkEvictor<E extends RawEvent> implements Evictor<E, GlobalWindow> {


    @Override
    public void evictBefore(Iterable<TimestampedValue<E>> iterable, int i, GlobalWindow globalWindow, EvictorContext evictorContext) {

    }

    @Override
    public void evictAfter(Iterable<TimestampedValue<E>> iterable, int i, GlobalWindow globalWindow, EvictorContext evictorContext) {

    }
}
