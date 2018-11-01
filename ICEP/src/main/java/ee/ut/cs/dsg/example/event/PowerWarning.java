/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package ee.ut.cs.dsg.example.event;

import ee.ut.cs.dsg.icep.event.IntervalEvent;

/**
 * @author MKamel
 */

public class PowerWarning extends IntervalEvent {


    public PowerWarning(long sts, long ets, double value, String valueDescriptor, String key) {
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
        if (obj instanceof PowerWarning) {
            PowerWarning other = (PowerWarning) obj;
            return value == other.getValue() && this.startTimestamp == other.getStartTimestamp() && this.endTimestamp == other.getEndTimestamp();
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Double.hashCode(value);
    }

    @Override
    public String toString()
    {
        String s = super.toString().replace("IntervalEvent","PowerWarning");
        return s;
    }



}
              
    
