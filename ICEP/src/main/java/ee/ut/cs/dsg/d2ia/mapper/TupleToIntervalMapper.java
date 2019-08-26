package ee.ut.cs.dsg.d2ia.mapper;

import ee.ut.cs.dsg.d2ia.event.IntervalEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;

import java.sql.Timestamp;

public class TupleToIntervalMapper<W extends IntervalEvent> implements MapFunction<Tuple6<String, Timestamp, Timestamp, Double, String, Integer>, W> {

    private Class<W> out;
    public TupleToIntervalMapper(Class<W> out)
    {
        this.out = out;
    }
    @Override
    public W map(Tuple6<String, Timestamp, Timestamp, Double, String, Integer> tuple) throws Exception {
        return out.getDeclaredConstructor( long.class, long.class, double.class, String.class, String.class).newInstance( tuple.f1.toInstant().getEpochSecond() , tuple.f2.toInstant().getEpochSecond() , tuple.f3 , tuple.f4, tuple.f0);
    }
}
