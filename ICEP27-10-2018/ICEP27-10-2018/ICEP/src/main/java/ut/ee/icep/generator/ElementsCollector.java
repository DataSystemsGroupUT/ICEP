/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ut.ee.icep.generator;


import org.apache.flink.annotation.Public;
import org.apache.flink.api.java.operators.Operator;
import org.apache.flink.cep.PatternSelectFunction;

import javax.naming.Context;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.eclipse.jdt.core.dom.Assignment;
import ut.ee.icep.events.*;
/**
 *
 * @author MKamel
 */
public class ElementsCollector<S extends RawEvent, W extends IntervalEvent > implements PatternSelectFunction<S,W>{
    private int minOccurs;
    private int maxOccurs;
    int flag;
    Class<W> out;

    String outv = "";

    double newvalue =0 ;
    int times;
    double summm =0;
  //  double sum=0;

    IntervalGenerator.Operator outvalue;

   static IntervalGenerator generator;


//    public ElementsCollector(IntervalGenerator generator
//            , Class<W> outClass, int minOccurs, int maxOccurs , int flag , IntervalGenerator.Operator outvalue)
//    {
//        this(outClass, minOccurs, maxOccurs, flag, outvalue);
//        this.generator = generator;
//    }


    public ElementsCollector( Class<W> outClass, int minOccurs, int maxOccurs , int flag , IntervalGenerator.Operator outvalue)
    {
        this.minOccurs = minOccurs;
        this.maxOccurs = maxOccurs;
        this.out = outClass;
        this.flag = flag;
        this.outvalue = outvalue;
    }

    public ElementsCollector(Class<W> outClass, int minOccurs, int maxOccurs , int flag , IntervalGenerator.Operator outvalue,int times)
    {
        this.minOccurs = minOccurs;
        this.maxOccurs = maxOccurs;
        this.out = outClass;
        this.flag = flag;
        this.outvalue = outvalue;
        this.times=times;
    }


    @Override
    public W select(Map<String, List<S>> map) throws Exception {

        if (flag == 0){

            long  firsttimestampe = map.get(Integer.toString(minOccurs)).get(0).getTimestamp();
            long  lasttimestampe= map.get(Integer.toString(maxOccurs)).get(0).getTimestamp();

            double First_value = map.get(Integer.toString(minOccurs)).get(0).getValue();
            double Last_value = map.get(Integer.toString(maxOccurs)).get(0).getValue();
          double sum=0;
            double avgValue;


            int rid = map.get(Integer.toString(minOccurs)).get(0).getRackID();

            for (int i = minOccurs; i <= maxOccurs; i++)
            {
                sum  = sum + map.get(Integer.toString(i)).get(0).getValue();
            }

            avgValue = sum/ (maxOccurs);

            if (outvalue == IntervalGenerator.Operator.First){
                outv = "First";
                newvalue =  First_value;
            }else if (outvalue == IntervalGenerator.Operator.Last){
                outv = "Last";
                newvalue =  Last_value;
            }
            else if (outvalue == IntervalGenerator.Operator.Average){
                outv = "Average";
                newvalue =  avgValue;
            }else if (outvalue == IntervalGenerator.Operator.Sum){
                outv = "Sum";
                newvalue =  sum;
            }

            W element = out.getDeclaredConstructor( int.class,long.class, long.class, double.class, String.class).newInstance(rid, firsttimestampe, lasttimestampe, newvalue, outv);
            return element;

        } else {
            long  firsttimestampe = map.get("1").get(0).getTimestamp();
            long  lasttimestampe= map.get("1").get(times- 1).getTimestamp();
            double avg  ;
            double sum =0 ;

            double First_value = map.get("1").get(0).getValue();
            double Last_value = map.get("1").get(times-1).getValue();
            int rid = map.get("1").get(0).getRackID();

            for (int i = 0; i < times; i++)
            {
                sum =sum + map.get("1").get(i).getValue();
            }

            avg = sum/ (times);

            if (outvalue == IntervalGenerator.Operator.First){
                outv = "First";
                newvalue =  First_value;
            }else if (outvalue == IntervalGenerator.Operator.Last){
                outv = "Last";
                newvalue =  Last_value;
            }
            else if (outvalue == IntervalGenerator.Operator.Average){
                outv = "Average";
                newvalue =  avg;
            }else if (outvalue == IntervalGenerator.Operator.Sum){
                outv = "Sum";
                newvalue =  sum;
            }

            W element = out.getDeclaredConstructor( int.class,long.class, long.class, double.class,String.class).newInstance(rid, firsttimestampe, lasttimestampe, newvalue, outv);
            if(element != null) {
            }
            return element;
        }


    }

    
}
