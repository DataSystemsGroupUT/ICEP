/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package ee.ut.cs.dsg.d2ia.event;


import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.StringValue;

import java.io.Serializable;

/**
 *
 * @author MKamel
 */

public class IntervalEvent  extends Tuple5<LongValue, LongValue, DoubleValue, StringValue, StringValue> {//implements Serializable {

//    protected long startTimestamp;
//    protected long endTimestamp;
//    protected double value;
//    protected String valueDescriptor;

//    protected String key;

    public IntervalEvent()
    {
        this(Long.MIN_VALUE, Long.MAX_VALUE, -1, "NONE", "dummy");
    }
    public IntervalEvent(long sts, long ets, double value, String valueDescriptor, String k)
    {

        this.f0 = new LongValue(sts);
        this.f1 = new LongValue(ets);
        this.f2 = new DoubleValue(value);
        this.f3 = new StringValue(valueDescriptor);
        this.f4 = new StringValue(k);
    }

    public long getStartTimestamp(){return f0.getValue();}

    public long getEndTimestamp(){return f1.getValue();}

    public double getValue(){ return f2.getValue();}

    public String getKey(){ return f4.getValue(); }

    public String getValueDescriptor(){return f3.getValue();}

//    public long getStartTimestamp()
//    {
//        return this.startTimestamp;
//    }
//    public long getEndTimestamp()
//    {
//        return this.endTimestamp;
//    }
//    public double getValue()
//    {
//        return this.value;
//    }
//
//    public String getValueDescriptor(){return this.valueDescriptor;}
//
//    public String getKey(){return this.key;}

    @Override
    public String toString()
    {
        return "IntervalEvent(start: "    + getStartTimestamp() + ", end: " + getEndTimestamp()+     ", value: " + getValue()+
                ", value description: "+ getValueDescriptor()+", key: "+getKey()+")";
    }

    @Override
    public boolean equals(Object other)
    {
        if (!(other instanceof  IntervalEvent))
            return false;
        IntervalEvent otherInterval = (IntervalEvent) other;
        return this.getStartTimestamp() == otherInterval.getStartTimestamp() && this.getEndTimestamp() == otherInterval.getEndTimestamp()
                && this.getValue() == otherInterval.getValue() && this.getValueDescriptor().equals(otherInterval.getValueDescriptor())
                && this.getKey().equals(otherInterval.getKey());
    }

}
