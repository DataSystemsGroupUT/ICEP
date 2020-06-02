/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package ee.ut.cs.dsg.example.event;

import ee.ut.cs.dsg.d2ia.event.IntervalEvent;

import java.io.Serializable;

/**
 * @author MKamel
 */

public class TemperatureWarning extends IntervalEvent implements Serializable {


    public TemperatureWarning(long sts, long ets, double value, String valueDescriptor, String key) {
        super(sts, ets, value, valueDescriptor, key);
    }

//    @Override
//    public long getStartTimestamp() {
//        return startTimestamp;
//    }
//
//    @Override
//    public long getEndTimestamp() {
//        return endTimestamp;
//    }
//
//    public double getValue() {
//        return value;
//    }


    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TemperatureWarning) {
            TemperatureWarning other = (TemperatureWarning) obj;
            return getValue() == other.getValue() && this.getStartTimestamp() == other.getStartTimestamp()
                    && this.getEndTimestamp() == other.getEndTimestamp() && this.getKey().equals(other.getKey());
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Double.hashCode(getValue());
    }

    @Override
    public String toString()
    {
        return super.toString().replace("IntervalEvent","TemperatureWarning");
    }



}
              
    
