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

public class IntervalEvent  implements Serializable {

    protected long startTimestamp;
    protected long endTimestamp;
    protected double value;
    protected String valueDescriptor;

    protected String key;
    public IntervalEvent(long sts, long ets, double value, String valueDescriptor, String k)
    {

        this.startTimestamp = sts;
        this.endTimestamp = ets;
        this.value = value;
        this.valueDescriptor = valueDescriptor;
        this.key=k;
    }


    public long getStartTimestamp()
    {
        return this.startTimestamp;
    }
    public long getEndTimestamp()
    {
        return this.endTimestamp;
    }
    public double getValue()
    {
        return this.value;
    }

    public String getValueDescriptor(){return this.valueDescriptor;}

    public String getKey(){return this.key;}

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
