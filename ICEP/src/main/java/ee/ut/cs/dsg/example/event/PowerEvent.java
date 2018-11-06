/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package ee.ut.cs.dsg.example.event;

import ee.ut.cs.dsg.d2ia.event.RawEvent;

/**
 *
 * @author MKamel
 */

public class PowerEvent extends RawEvent {
    
    public PowerEvent(String id, long ts, double v) {
        super(id, ts, v);
    }

      @Override
    public boolean equals(Object obj) {
        if (obj instanceof PowerEvent) {
            PowerEvent powerEvent = (PowerEvent) obj;
            return powerEvent.canEquals(this) && super.equals(powerEvent) && value == powerEvent.value; // && id == powerEvent.id;
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return (41 * super.hashCode() + Double.hashCode(value));

    }



    public boolean canEquals(Object obj) {
        return obj instanceof PowerEvent;
    }

    @Override
    public String toString() {
        return "PowerEvent(" + value + " , " + timestamp + ")";
    }
}

