/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ee.ut.cs.dsg.d2ia.generator;

/*
  @author MKamel
 */

import ee.ut.cs.dsg.d2ia.condition.AbsoluteCondition;
import ee.ut.cs.dsg.d2ia.condition.Condition;
import ee.ut.cs.dsg.d2ia.condition.Operand;
import ee.ut.cs.dsg.d2ia.condition.RelativeCondition;
import ee.ut.cs.dsg.d2ia.event.IntervalEvent;
import ee.ut.cs.dsg.d2ia.event.RawEvent;
import ee.ut.cs.dsg.d2ia.evictor.WatermarkEvictor;
import ee.ut.cs.dsg.d2ia.mapper.TupleToIntervalMapper;
import ee.ut.cs.dsg.d2ia.processor.D2IAHomogeneousIntervalProcessorFunction;
import ee.ut.cs.dsg.d2ia.trigger.GlobalWindowEventTimeTrigger;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;



import java.io.Serializable;
import java.sql.Timestamp;


import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;


public class HomogeneousIntervalGenerator<S extends RawEvent, W extends IntervalEvent> implements Serializable {

    private Class<S> sourceTypeClass;
    private Class<W> targetTypeClass;
    private DataStream<S> sourceStream;
    private DataStream<W> targetStream;
    private String runMode="CEP";
    // private KeyedStream<S, String> keyedSourceStream;


    // used to form the CEP rule
    private int minOccurs = 1;
    private int maxOccurs = Integer.MAX_VALUE;
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


    public HomogeneousIntervalGenerator outputValue(Operand outvalue) {
        this.outputValueOperand = outvalue;
        return this;
    }


    /**

     */


    private void validate() throws Exception {
        if (this.sourceStream == null) {
            throw new Exception("Source Stream must be defined");
        }

        if(minOccurs < 1 )
        {
            throw new Exception("Minimum occurrences cannot be less than 1");
        }

        if (maxOccurs < minOccurs)
        {
            throw new Exception("Max occurrences cannot be less than minimum occurrences");
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
        if (runMode.equalsIgnoreCase("sql") && this.within != null && this.within.toMilliseconds() < 1000)
        {
            throw new Exception("Flink SQL does not support time unit granularity below SECOND");
        }

    }

    public DataStream<W> runWithSQL(StreamExecutionEnvironment env) throws Exception {


        runMode="SQL";
        validate();
        TimeCharacteristic tC = env.getStreamTimeCharacteristic();
        String queryString = buildQueryString();

        // start registering the streams

            KeyedStream keyedStream;
            if (!(sourceStream instanceof KeyedStream)) {
                keyedStream = sourceStream.keyBy((KeySelector<S, String>) RawEvent::getKey);
            } else {
                keyedStream = (KeyedStream) sourceStream;
            }


        // Define the type info to avoid type info vanishing during transformations
        TupleTypeInfo<Tuple3<String, Double, Long>> inputTupleInfoEventTime = new TupleTypeInfo<>(
                Types.STRING(),
                Types.DOUBLE(),
                Types.LONG()
        );

        TupleTypeInfo<Tuple3<String, Double, Long>> inputTupleInfoProcessingTime = new TupleTypeInfo<>(
                Types.STRING(),
                Types.DOUBLE()
        );



        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);//.getTableEnvironment(env);

        if (env.getStreamTimeCharacteristic() == TimeCharacteristic.EventTime) {
            tableEnv.registerDataStream("RawEvents",
                    keyedStream.map((MapFunction<S, Tuple3<String, Double, Long>>) event -> new Tuple3<>(event.getKey(), event.getValue(), event.getTimestamp())).returns(inputTupleInfoEventTime),
                    "ID, val, eventTime.rowtime"
            );
        }
        else
        {
            tableEnv.registerDataStream("RawEvents",
                    keyedStream.map((MapFunction<S, Tuple2<String, Double>>) event -> new Tuple2<>(event.getKey(), event.getValue())).returns(inputTupleInfoProcessingTime),
                    "ID, val, eventTime.proctime"
            );
        }




        Table intervalResult = tableEnv.sqlQuery(queryString);

        TupleTypeInfo<Tuple6<String, Timestamp, Timestamp, Double, String, Integer>> tupleTypeInterval = new TupleTypeInfo<>(
                Types.STRING(),
                Types.SQL_TIMESTAMP(),
                Types.SQL_TIMESTAMP(),
                Types.DOUBLE(),
                Types.STRING(),
                Types.INT()
        );

        DataStream<Tuple6<String, Timestamp, Timestamp, Double, String, Integer>> queryResultAsStream = tableEnv.toAppendStream(intervalResult, tupleTypeInterval);
       // queryResultAsStream.print();
        //(MapFunction<Tuple5<String, Timestamp, Timestamp, Double, String>, W>) tuple -> targetTypeClass.getDeclaredConstructor(long.class, long.class, double.class, String.class, String.class).newInstance(tuple.f1.getTime(), tuple.f2.getTime(), tuple.f3, tuple.f4, tuple.f0)
        return queryResultAsStream.map(new TupleToIntervalMapper<>(targetTypeClass)).returns(targetTypeClass);


    }

    private String buildQueryString() {
        String skipStrategy = "AFTER MATCH ";
        if (onlyMaximalIntervals)
            skipStrategy += "SKIP PAST LAST ROW";
        else
            skipStrategy += "SKIP TO NEXT ROW";


        StringBuilder sqlQuery = new StringBuilder();

        sqlQuery.append("Select ID, sts, ets, intervalValue,valueDescription, intvDuration from RawEvents Match_Recognize (\n");
        sqlQuery.append("PARTITION BY ID\n");
        sqlQuery.append("ORDER BY eventTime\n");
        sqlQuery.append("MEASURES\n");
        sqlQuery.append("A.ID AS id,\n");
        sqlQuery.append("FIRST(A.eventTime) AS sts,\n");
        sqlQuery.append("LAST(A.eventTime) AS ets,\n");

        //get the value

        if (outputValueOperand == Operand.First) {
            sqlQuery.append("First(A.val) As intervalValue");

        } else if (outputValueOperand == Operand.Last) {
            sqlQuery.append("Last(A.val) As intervalValue");
        } else if (outputValueOperand == Operand.Average) {
            sqlQuery.append("AVG(A.val) As intervalValue");
        } else if (outputValueOperand == Operand.Sum) {
            sqlQuery.append("SUM(A.val) As intervalValue");
        } else if (outputValueOperand == Operand.Max) {
            sqlQuery.append("MAX(A.val) As intervalValue");
        } else if (outputValueOperand == Operand.Min) {
            sqlQuery.append("MIN(A.val) As intervalValue");
        }
        //Just for formatting
        sqlQuery.append(",\n");

        // value description
        sqlQuery.append(String.format("'%s' AS valueDescription,\n", outputValueOperand.toString()));

        // just for testing purposes
        sqlQuery.append("TIMESTAMPDIFF(SECOND, FIRST(A.eventTime), LAST(A.eventTime)) AS intvDuration \n");

        //Skip strategy
        sqlQuery.append(skipStrategy).append("\n");
        // pattern

        if (minOccurs > 1 && maxOccurs != Integer.MAX_VALUE) // both upper and lower bounds set
        {
            sqlQuery.append(String.format("PATTERN (A{%d,%d} B)\n", minOccurs, maxOccurs));
        } else if (minOccurs > 1) {
            sqlQuery.append(String.format("PATTERN (A{%d,} B)\n", minOccurs));
        } else if (maxOccurs != Integer.MAX_VALUE) {
            sqlQuery.append(String.format("PATTERN (A{1,%d} B)\n", maxOccurs));
        } else // we put one or more
        {
            sqlQuery.append("PATTERN (A+ B)\n");
        }



        // define clause
        sqlQuery.append("DEFINE\n");
        String conditionString;
        if (condition instanceof AbsoluteCondition ) {
            conditionString = condition.toString().replace("!", "not")
                    .replace("==", "=")
                    .replace("!=", "<>")
                    .replace("&&", " AND ")
                    .replace("||", " OR ")
                    .replace("Math.abs", "ABS")
                    .replace("value", "val");

            if (within == null)

                sqlQuery.append(String.format("A as A.%s,\n", conditionString));
            else
            {

                sqlQuery.append(String.format("A as (A.%s and LAST(A.val,1) IS NULL) OR ( A.%s AND TIMESTAMPDIFF(SECOND, LAST(A.eventTime,1), A.eventTime) <= %d),\n", conditionString,conditionString,(long)Math.floor(this.within.toMilliseconds()/1000)));
            }


        } else {

//            throw new NotImplementedException();
            conditionString = condition.toString();
            String startCondition;
            String relativeCondition;
            startCondition = conditionString.substring(0, conditionString.indexOf(" Relative"));
            relativeCondition = conditionString.substring(conditionString.indexOf(" Relative ") + 10);
            startCondition = startCondition.replace("!", "not")
                    .replace("==", "=")
                    .replace("!=", "<>")
                    .replace("&&", " AND ")
                    .replace("||", " OR ")
                    .replace("Math.abs", "ABS")
                    .replace("value", "A.val");

            sqlQuery.append(String.format("A as (%s and LAST(A.val,1) IS NULL) OR ", startCondition));

            relativeCondition = relativeCondition.replace("!", "not")
                    .replace("==", "=")
                    .replace("!=", "<>")
                    .replace("&&", " AND ")
                    .replace("||", " OR ")
                    .replace("valueMath.abs", "ABS")
                    .replace("value", "A.val")
                    .replace("last", "LAST(A.val,1)")
                    .replace("first", "FIRST(A.val)")
                    .replace("avg", "AVG(A.val)")
                    .replace("sum", "SUM(A.val)")
                    .replace("min", "MIN(A.val)")
                    .replace("max", "MAX(A.val)")
                    ;

            RelativeCondition relCondition = (RelativeCondition) condition;
            String parsedLHSCond="";
            String parsedRHSCond="";
            if (relCondition.getRelativeLHS() instanceof AbsoluteCondition)
            {
                parsedLHSCond = ((AbsoluteCondition) relCondition.getRelativeLHS()).parse(-1,-2,-3,-4,-5,-6,-7);
                parsedLHSCond = parsedLHSCond.replace("Math.abs","ABS")
                        .replace("-7.0", "A.val")
                        .replace("-1.0","FIRST(A.val)")
                        .replace("-2.0","LAST(A.val,1)")
                        .replace("-3.0","MIN(A.val)")
                        .replace("-4.0","MAX(A.val)")
                        .replace("-5.0","SUM(A.val)")
                        .replace("-6.0","COUNT(A.val)");

                //System.out.println(parsedCond);
            }

            if (relCondition.getRelativeRHS() instanceof AbsoluteCondition)
            {
                parsedRHSCond = ((AbsoluteCondition) relCondition.getRelativeRHS()).parse(-1,-2,-3,-4,-5,-6,-7);
                parsedRHSCond = parsedRHSCond.replace("Math.abs","ABS")
                        .replace("-7.0", "A.val")
                        .replace("-1.0","FIRST(A.val)")
                        .replace("-2.0","LAST(A.val,1)")
                        .replace("-3.0","MIN(A.val)")
                        .replace("-4.0","MAX(A.val)")
                        .replace("-5.0","SUM(A.val)")
                        .replace("-6.0","COUNT(A.val)");

            }
            if (parsedLHSCond.length() > 0 && parsedRHSCond.length() > 0)
            {
                relativeCondition = parsedLHSCond + " " + relCondition.getRelativeOperator().toString()+ " " + parsedRHSCond;
            }
            else if (parsedLHSCond.length() > 0)
            {
                relativeCondition = parsedLHSCond + " " + relCondition.getRelativeOperator().toString() + " " + relCondition.getRelativeRHS().toString();
            }
            else if (parsedRHSCond.length() > 0 )
            {
                relativeCondition = relCondition.getRelativeLHS().toString() + " " + relCondition.getRelativeOperator().toString() + " " + parsedRHSCond;
            }

            relativeCondition = relativeCondition.replace("!", "not")
                    .replace("==", "=")
                    .replace("!=", "<>")
                    .replace("&&", " AND ")
                    .replace("||", " OR ")
                    .replace("valueMath.abs", "ABS")
                    .replace("value", "A.val")
                    .replace("last", "LAST(A.val,1)")
                    .replace("first", "FIRST(A.val)")
                    .replace("avg", "AVG(A.val)")
                    .replace("sum", "SUM(A.val)")
                    .replace("min", "MIN(A.val)")
                    .replace("max", "MAX(A.val)")
            ;
            if (within != null)
                sqlQuery.append("(");

            sqlQuery.append(String.format("(%s)", relativeCondition));

            if (within != null) {
                sqlQuery.append(String.format(" AND (TIMESTAMPDIFF(SECOND, LAST(A.eventTime,1), A.eventTime) <= %d)", (long)Math.floor(this.within.toMilliseconds()/1000)));

            }
            sqlQuery.append(",\n");
        }
        sqlQuery.append("B As true\n");

        sqlQuery.append(")");

        return sqlQuery.toString();
    }

    public DataStream<W> runWithCEP() throws Exception {
        runMode = "CEP";

        // Check that minimum input is provided to generate an interval
        validate();

        AfterMatchSkipStrategy skipStrategy;

        if (onlyMaximalIntervals)
            skipStrategy = AfterMatchSkipStrategy.skipToLast("2");//.skipPastLastEvent();//.skipPastLastEvent();//.noSkip();//.skipToLast("1");
        else
            skipStrategy = AfterMatchSkipStrategy.noSkip();

        Pattern<S, S> interval = Pattern.<S>begin("1", skipStrategy).subtype(sourceTypeClass);
//        Pattern<S, S> interval = Pattern.<S>begin("1").subtype(sourceTypeClass);
        if (minOccurs > 1 && maxOccurs != Integer.MAX_VALUE) // both upper and lower bounds set
        {
            interval = interval.times(minOccurs, maxOccurs);
        } else if (minOccurs > 1) {
            interval = interval.timesOrMore(minOccurs);
        } else if (maxOccurs != Integer.MAX_VALUE) {
            interval = interval.times(1, maxOccurs);
        } else // we put one or more
        {
            interval = interval.oneOrMore();
        }

 //       if (!onlyMaximalIntervals)
            interval = interval.greedy();
        if (within != null)
            interval = interval.within(within);

        //   if (condition instanceof AbsoluteCondition) {
        //  if (condition != null && minOccurs == Integer.MAX_VALUE && maxOccurs == Integer.MIN_VALUE)
        if (condition instanceof RelativeCondition)
            interval = interval.until(new RelativeIterativeCondition<>(condition, RelativeIterativeCondition.ConditionContainer.Until));
        else
            interval = interval.where(new AbsoluteSimpleCondition<>((AbsoluteCondition) condition));

        interval = interval.followedBy("2").where(new SimpleCondition<S>() {
            @Override
            public boolean filter(S s) throws Exception {
                return true;
            }
        });
//        else
//            interval = interval.where(new RelativeIterativeCondition<>(condition, RelativeIterativeCondition.ConditionContainer.Where));
        PatternStream<S> pattern;
        if (!(sourceStream instanceof KeyedStream)) {
            pattern = CEP.pattern(sourceStream.keyBy((KeySelector<S, String>) RawEvent::getKey), interval);
        } else
            pattern = CEP.pattern(sourceStream, interval);

        targetStream = pattern.select(new HomogeneousIntervalElementsCollector<>(targetTypeClass, outputValueOperand), TypeInformation.of(targetTypeClass));


        return targetStream;


        //  return applyMaximalInterval();
    }

    @Deprecated
    private DataStream<W> applyMaximalInterval() {
        if (!onlyMaximalIntervals)
            return targetStream;
        else {
            return targetStream.keyBy((KeySelector<W, String>) IntervalEvent::getKey).window(TumblingProcessingTimeWindows.of(within)).apply((WindowFunction<W, W, String, TimeWindow>) (s, timeWindow, iterable, collector) -> {
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
            });
        }
    }


    public DataStream<W> runwithWindow(long windowWidth) throws Exception
    {
        runMode="Window";
        validate();

        // I am changing the window into an event time tumbling window as I can see a scalability issue.
        // Every time I increase the number of workers, the jobs takes longer, totally opposite to what's expected.
        if (sourceStream instanceof KeyedStream) {
            targetStream = ((KeyedStream) sourceStream).window(TumblingEventTimeWindows.of(Time.seconds(windowWidth)))
                    //  .trigger(new GlobalWindowEventTimeTrigger())
//                    .trigger(CountTrigger.of(1))
//                    .evictor(CountEvictor.of(0, true))
                    .process(new D2IAHomogeneousIntervalProcessorFunction<>(minOccurs, maxOccurs, condition, within, onlyMaximalIntervals, outputValueOperand, sourceTypeClass, targetTypeClass), TypeInformation.of(targetTypeClass));
        } else {
            targetStream = sourceStream.keyBy((KeySelector<S, String>) RawEvent::getKey).window(TumblingEventTimeWindows.of(Time.seconds(windowWidth)))
                    //.trigger(new GlobalWindowEventTimeTrigger())
//                    .trigger(CountTrigger.of(1))
//                    .evictor(CountEvictor.of(0,true))
                    . process(new D2IAHomogeneousIntervalProcessorFunction<>(minOccurs, maxOccurs, condition, within, onlyMaximalIntervals, outputValueOperand, sourceTypeClass,targetTypeClass), TypeInformation.of(targetTypeClass));
        }

        return targetStream;
//        return applyMaximalInterval();
    }
    public DataStream<W> runWithWindow() throws Exception {

        return runwithWindow(10L);
    }


}
