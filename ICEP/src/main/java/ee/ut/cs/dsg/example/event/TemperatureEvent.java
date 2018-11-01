/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package ee.ut.cs.dsg.example.event;

import ee.ut.cs.dsg.icep.event.RawEvent;

/**
 *
 * @author MKamel
 */

public class TemperatureEvent extends RawEvent {

       public TemperatureEvent(String id , long ts, double v) {
        super(id, ts, v);
      }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TemperatureEvent) {
            TemperatureEvent other = (TemperatureEvent) obj;
            return other.canEquals(this) && super.equals(other) && value == other.value;
        } else {
            return false;
        }
    }
    
    @Override
    public int hashCode() {
        return 41 * super.hashCode() + Double.hashCode(value);
    }

    public boolean canEquals(Object obj){
        return obj instanceof TemperatureEvent;
    }

    @Override
    public String toString() {
        return "TemperatureEvent("  + value + " , " + timestamp + ")";
    }
}
