/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ut.ee.icep.generator;


import org.apache.flink.cep.PatternSelectFunction;

import javax.naming.Context;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import ut.ee.icep.events.*;
/**
 *
 * @author MKamel
 */
public class ElementsCollector<S extends RawEvent, W extends IntervalEvent> implements PatternSelectFunction<S,W>{
    private int minOccurs;
    private int maxOccurs;
    int flag;

    public ElementsCollector(int minOccurs, int maxOccurs , int flag)
    {
        this.minOccurs = minOccurs;
        this.maxOccurs = maxOccurs;
        this.flag = flag;
    }

    @Override
    public W select(Map<String, List<S>> map) throws Exception {

        if (flag == 0){
            long  firsttimestampe = map.get("1").get(0).getTimestamp();
            long  lasttimestampe= map.get(Integer.toString(maxOccurs)).get(0).getTimestamp();

            double value = map.get("1").get(0).getValue();
            double avgValue;

            for (int i = minOccurs+ 1; i <= maxOccurs; i++)
            {
                value  = value + map.get(Integer.toString(i)).get(0).getValue();
            }
            avgValue = value/ (maxOccurs);
            W element = (W) new IntervalEvent(firsttimestampe, lasttimestampe, avgValue);
            return element;

        } else {

            double value= 0;
            for (int i = minOccurs; i < maxOccurs; i++)
            {
                value  = value + map.get("1").get(i- 1).getValue();
            }

            long firsttimestampe = map.get("1").get(0).getTimestamp();
            long  lasttimestampe= map.get("1").get(maxOccurs- 1).getTimestamp();
            double avgValue = value/ (maxOccurs);

            W element = (W) new IntervalEvent(firsttimestampe, lasttimestampe, avgValue);

            return element;

        }


    }
    
}
