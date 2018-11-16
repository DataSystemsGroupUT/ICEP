/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ee.ut.cs.dsg.d2ia.generator;

/**
 * @author MKamel
 */

import ee.ut.cs.dsg.d2ia.condition.AbsoluteCondition;
import ee.ut.cs.dsg.d2ia.condition.Condition;
import ee.ut.cs.dsg.d2ia.condition.Operand;
import ee.ut.cs.dsg.d2ia.event.IntervalEvent;
import ee.ut.cs.dsg.d2ia.event.RawEvent;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;

import java.io.Serializable;

import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


public class HomogeneousIntervalGenerator<S extends RawEvent, W extends IntervalEvent> implements Serializable {

    private Class<S> sourceTypeClass;
    private Class<W> targetTypeClass;
    private DataStream<S> sourceStream;
    private DataStream<W> targetStream;
    // private KeyedStream<S, String> keyedSourceStream;


    // used to form the CEP rule
    private int minOccurs = Integer.MAX_VALUE;
    private int maxOccurs = Integer.MIN_VALUE;
    // used to control the time window for the match
    private Time within;
//	Operator operator;
//
//	Operator outvalue;

//    private boolean groupStreamByKey = false;


//	static 	double val = 0;

    //String contype;

    // Used to evaluate the relative condition
    private Condition condition;


    // used to control the output value
    private Operand outputValueOperand;


    private boolean onlyMaximalIntervals;
//	static ArrayList<Double> places = new ArrayList<Double>();


    //static double compareValue;

//	public static final String VALUE = "tempValue";
//	private static ArrayList<Integer> eventIndecies = new ArrayList();


    public HomogeneousIntervalGenerator sourceType(Class<S> sourceTyp) {
        this.sourceTypeClass = sourceTyp;
        return this;
    }

    public HomogeneousIntervalGenerator source(DataStream<S> srcStream) {
        this.sourceStream = srcStream;
        return this;
    }

//    public HomogeneousIntervalGenerator target(DataStream<W> trgtStream) {
//        this.targetStream = trgtStream;
//        return this;
//    }

    public HomogeneousIntervalGenerator targetType(Class<W> targetTyp) {
        this.targetTypeClass = targetTyp;
        return this;
    }

    public HomogeneousIntervalGenerator within(Time t) {
        this.within = t;
        return this;
    }

    public HomogeneousIntervalGenerator condition(Condition cnd) {
        this.condition = cnd;
        return this;
    }

//    public HomogeneousIntervalGenerator groupByKey(boolean b) {
//        this.groupStreamByKey = b;
//        return this;
//    }

    public HomogeneousIntervalGenerator minOccurrences(int min) {
        this.minOccurs = min;
        return this;
    }

    public HomogeneousIntervalGenerator maxOccurrences(int max) {
        this.maxOccurs = max;
        return this;
    }

    public HomogeneousIntervalGenerator produceOnlyMaximalIntervals(boolean maximal) {
        this.onlyMaximalIntervals = maximal;
        return this;

    }
//	public HomogeneousIntervalGenerator condition(int min, int max, Operator op, double value , Time te)
//	{
//		this.minOccurs = min;
//		this.maxOccurs = max;
//		this.operator = op;
//		this.timee = te;
//		this.compareValue = value;
//		return this;
//	}

    public HomogeneousIntervalGenerator outputValue(Operand outvalue) {
        this.outputValueOperand = outvalue;
        return this;
    }

//	public HomogeneousIntervalGenerator conditionType(String  conditiontype)
//	{
//		this.contype = conditiontype;
//		return this;
//	}
//
//	public String enumInIff(Operator opp){
//		if (operator == Operator.GreaterThan){
//			return ">";
//		}
//		else if (operator == Operator.Equals){
//			return "==";
//		}
//		else if (operator == Operator.LessThan){
//			return "<";
//		}
//		else if (operator == Operator.LessThanEqual){
//			return "<=";
//		}
//		else if (operator == Operator.GreaterThanEqual){
//			return ">=";
//		}
//		else if (operator == Operator.NotEqual){
//			return "!=";
//		}
//		else if (operator == Operator.First){
//			return "First";
//		}
//		else if (operator == Operator.Last){
//			return "Last";
//		}
//		else if (operator == Operator.Average){
//			return "Average";
//		}else if (operator == Operator.Sum){
//			return "Sum";
//		}
//		else {
//			return "invalid operator";
//		}
//	}

    /**

     */


    private void validate() throws Exception {
        if (this.sourceStream == null) {
            throw new Exception("Source Stream must be defined");
        }

//        if (this.targetStream == null) {
//            throw new Exception("target stream must be defined");
//        }

        if (this.condition == null && this.minOccurs == Integer.MAX_VALUE && this.maxOccurs == Integer.MIN_VALUE) {
            throw new Exception("Either a condition must be defined or the min and max occurrences of events must be set");
        }
        if (condition instanceof AbsoluteCondition && condition.getRHS() instanceof Operand) {
            throw new Exception("Absolute condition must have its right hand operand as a constant");
        }

        if (outputValueOperand == null) {
            throw new Exception("Output value operand must be defined");
        }

    }


    public DataStream<W> run() throws Exception {

        // Check that minimum input is provided to generate an interval
        validate();

        AfterMatchSkipStrategy skipStrategy = AfterMatchSkipStrategy.noSkip();//.skipPastLastEvent();//.skipToLast("1");
        Pattern<S, S> interval = Pattern.<S>begin("1",skipStrategy).subtype(sourceTypeClass);
//        Pattern<S, S> interval = Pattern.<S>begin("1").subtype(sourceTypeClass);
        if (minOccurs != Integer.MAX_VALUE && maxOccurs != Integer.MIN_VALUE) // both upper and lower bounds set
        {
            interval = interval.times(minOccurs, maxOccurs);
        } else if (minOccurs != Integer.MAX_VALUE) {
            interval = interval.timesOrMore(minOccurs).greedy();
        } else if (maxOccurs != Integer.MIN_VALUE) {
            interval = interval.times(1, maxOccurs);
        } else // we put one or more
        {
            interval = interval.oneOrMore().greedy();
        }

        if (within != null)
            interval = interval.within(within);

        //   if (condition instanceof AbsoluteCondition) {
      //  if (condition != null && minOccurs == Integer.MAX_VALUE && maxOccurs == Integer.MIN_VALUE)
            interval = interval.until(new MyIterativeCondition<>(condition, MyIterativeCondition.ConditionContainer.Until));
//        else
//            interval = interval.where(new MyIterativeCondition<>(condition, MyIterativeCondition.ConditionContainer.Where));

        PatternStream<S> pattern = CEP.pattern(sourceStream.keyBy(new KeySelector<S, String>() {
            @Override
            public String getKey(S value) throws Exception {

                return value.getKey();

            }
        }), interval);
        targetStream = pattern.select(new HomogeneousIntervalElementsCollector<>(targetTypeClass, outputValueOperand), TypeInformation.of(targetTypeClass));


        /** Following part commented by Ahmed */


        if (!onlyMaximalIntervals)
            return targetStream;
        else {
            DataStream<W> filteredStream = targetStream.keyBy(new KeySelector<W, String>() {

                @Override
                public String getKey(W w) throws Exception {
                    return w.getKey();
                }
            }).window(TumblingEventTimeWindows.of(within)).apply(new WindowFunction<W, W, String, TimeWindow>() {
                @Override
                public void apply(String s, TimeWindow timeWindow, Iterable<W> iterable, Collector<W> collector) throws Exception {
                    boolean containerFound;
                    //       System.out.println("TEST");
                    for (W out : iterable) {
                        containerFound = false;
                        for (W in : iterable) {
                            Match.MatchType mt = Match.getMatchType(out, in);
//                            System.out.println(out.toString());
//                            System.out.println(in.toString());
//                            System.out.println(mt.toString());
                            if (mt == Match.MatchType.Equals)
                                continue;
                            if (mt == Match.MatchType.Starts || mt == Match.MatchType.During || mt == Match.MatchType.Finishes)
                                containerFound = true;
                        }
                        if (!containerFound) {
                            //  System.out.println("SELECT "+out);
                            collector.collect(out);
                        }
                    }
                }
            });
            return filteredStream;
        }
    }


}
