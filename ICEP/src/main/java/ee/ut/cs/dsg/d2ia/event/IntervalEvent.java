/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package ee.ut.cs.dsg.d2ia.event;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;

import java.io.Serializable;

/**
 *
 * @author MKamel
 */

public class IntervalEvent  extends Tuple5<String, Double, Long, Long, String> { //implements Serializable {

//    protected long startTimestamp;
//    protected long endTimestamp;
//    protected double value;
//    protected String valueDescriptor;

//    protected String key;
    public IntervalEvent(long sts, long ets, double value, String valueDescriptor, String k)
    {
        super(k, value, sts, ets, valueDescriptor);
//        this.startTimestamp = sts;
//        this.endTimestamp = ets;
//        this.value = value;
//        this.valueDescriptor = valueDescriptor;
//        this.key=k;
    }


    public long getStartTimestamp()
    {
        return f2;
    }
    public long getEndTimestamp()
    {
        return f3;
    }
    public double getValue()
    {
        return f1;
    }

    public String getValueDescriptor(){return f4;}

    public String getKey(){return f0;}

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
