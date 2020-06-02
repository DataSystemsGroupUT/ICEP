/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package ee.ut.cs.dsg.d2ia.event;

import org.apache.flink.api.java.tuple.Tuple3;

import java.io.Serializable;

/**
 *
 * @author MKamel
 */
public abstract class RawEvent extends Tuple3<String, Double, Long> {// implements Serializable {

//    protected long timestamp;
//    protected double value;
//    protected String key;

    public RawEvent(long ts, double v)
    {
        super("dummy", v, ts);
//        timestamp = ts;
//        value = v;
//        key = "dummy";
    }
    protected RawEvent(String k, long ts, double v)
    {
        super(k,v,ts);
//        this.key = k;
//        timestamp = ts;
//        value = v;
    }


    public long getTimestamp()
    {
        return this.f2;
    }

    public double getValue()
    {
        return this.f1;
    }

    public String getKey(){ return this.f0;}

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof RawEvent)) return false;
        RawEvent other = (RawEvent) obj;

        return other.getTimestamp() == this.getTimestamp() && other.getValue() == this.getValue() && other.getKey().equals(this.getKey());
    }
}
