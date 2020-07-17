/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package ee.ut.cs.dsg.d2ia.event;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.StringValue;

import java.io.Serializable;

/**
 *
 * @author MKamel
 */
public abstract class RawEvent extends Tuple3<StringValue, LongValue, DoubleValue> {// implements Serializable {

//    protected long timestamp;
//    protected double value;
//    protected String key;

    public RawEvent()
    {
        f0 = new StringValue("dummy");
        f1 = new LongValue(Long.MIN_VALUE);
        f2 = new DoubleValue(-1d);

    }
    public RawEvent(long ts, double v)
    {

        this("dummy", ts, v);
//        timestamp = ts;
//        value = v;
//        key = "dummy";

    }
    protected RawEvent(String k, long ts, double v)
    {
        this.f1 = new LongValue(ts);
        this.f0 = new StringValue(k);
        this.f2 = new DoubleValue(v);
    }

    public long getTimestamp(){return f1.getValue();}

    public String getKey() { return f0.getValue(); }

    public double getValue() {return f2.getValue();}

//    public long getTimestamp()
//    {
//        return timestamp;
//    }
//
//    public double getValue()
//    {
//        return value;
//    }
//
//    public String getKey(){ return key;}

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof RawEvent)) return false;
        RawEvent other = (RawEvent) obj;

        return other.getTimestamp() == this.getTimestamp() && other.getValue() == this.getValue() && other.getKey().equals(this.getKey());
    }
}
