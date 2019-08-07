package ee.ut.cs.dsg.d2ia.evictor;

import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;

import java.util.Iterator;

public class WatermarkEvictor implements Evictor<Object, GlobalWindow> {



    @Override
    public void evictBefore(Iterable<TimestampedValue<Object>> iterable, int i, GlobalWindow globalWindow, EvictorContext evictorContext) {

    }

    @Override
    public void evictAfter(Iterable<TimestampedValue<Object>> iterable, int i, GlobalWindow globalWindow, EvictorContext evictorContext) {

        for (Iterator<TimestampedValue<Object>> iterator = iterable.iterator(); iterator.hasNext();){
            TimestampedValue<Object> v =  iterator.next();
            if (v.getTimestamp() <= evictorContext.getCurrentWatermark())
            {
                iterator.remove();
            }

        }

    }
}
