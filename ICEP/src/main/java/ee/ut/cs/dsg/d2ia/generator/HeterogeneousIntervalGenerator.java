package ee.ut.cs.dsg.d2ia.generator;

import ee.ut.cs.dsg.d2ia.condition.AbsoluteCondition;
import ee.ut.cs.dsg.d2ia.condition.Condition;
import ee.ut.cs.dsg.d2ia.condition.Operand;
import ee.ut.cs.dsg.d2ia.condition.RelativeCondition;
import ee.ut.cs.dsg.d2ia.event.IntervalEvent;
import ee.ut.cs.dsg.d2ia.event.RawEvent;
import ee.ut.cs.dsg.d2ia.exception.NotSupportedYetException;
import ee.ut.cs.dsg.d2ia.mapper.TupleToIntervalMapper;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.Serializable;
import java.sql.Timestamp;


import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;



public class HeterogeneousIntervalGenerator<S extends RawEvent, E extends RawEvent, F extends RawEvent, W extends IntervalEvent> implements Serializable {

    private Class<S> startTypeClass;
    private Class<E> endTypeClass;

    private Class<F> forbiddenTypeClass;
    private Class<W> targetTypeClass;


    private DataStream<S> startStream;
    private DataStream<E> endStream;
    private DataStream<W> targetStream;
    private DataStream<F> forbiddenStream;

    private Condition startStreamCondition;
    private Condition endStreamCondition;

    private Time within;


    public HeterogeneousIntervalGenerator startEventType(Class<S> srctype) {
        this.startTypeClass = srctype;
        return this;
    }

    public HeterogeneousIntervalGenerator endEventType(Class<E> srctype) {
        this.endTypeClass = srctype;
        return this;
    }

    public HeterogeneousIntervalGenerator startSource(DataStream<S> srstream) {
        this.startStream = srstream;
        return this;
    }

    public HeterogeneousIntervalGenerator endSource(DataStream<E> endStream) {
        this.endStream = endStream;
        return this;
    }

    public HeterogeneousIntervalGenerator forbiddenStream(DataStream<F> forbiddenStream) {
        //TODO: Generalize to make a list of forbidden streams
        this.forbiddenStream = forbiddenStream;
        return this;
    }


    public HeterogeneousIntervalGenerator target(DataStream<W> trgtStream) {
        this.targetStream = trgtStream;
        return this;
    }

    public HeterogeneousIntervalGenerator targetType(Class<W> targetTyp) {
        this.targetTypeClass = targetTyp;
        return this;
    }

    public HeterogeneousIntervalGenerator startStreamCondition(AbsoluteCondition cond) {
        this.startStreamCondition = cond;
        return this;
    }

    public HeterogeneousIntervalGenerator endStreamCondition(Condition cond) {
        this.endStreamCondition = cond;
        return this;
    }

    public HeterogeneousIntervalGenerator within(Time t) {
        this.within = t;
        return this;
    }

    private void validate() throws Exception {
        if (startStream == null) {
            throw new Exception("Stream of start events must be defined");
        }

        if (endStream == null) {
            throw new Exception("Stream of end events must be defined");
        }

    }

    public DataStream<W> runWithGlobalWindow() throws Exception
    {
        DataStream<RawEvent> all = prepareInputStreams();
        throw new NotImplementedException();
    }
    public DataStream<W> runWithSQL(StreamExecutionEnvironment env) throws Exception
    {
        if (forbiddenStream!=null)
        {
            throw new NotSupportedYetException("Forbidden elements on a Heterogeneous interval should map to a pattern with negation. This currently not supported by Flink Match Recognize");
        }
        DataStream<RawEvent> all = prepareInputStreams();

        DataStream<Tuple4<String, Double, String, Long>> streamAsTuple = all.map(new MapFunction<RawEvent, Tuple4<String, Double, String, Long>>() {
            @Override
            public Tuple4<String, Double, String, Long> map(RawEvent rawEvent) throws Exception {
                return new Tuple4<>(rawEvent.getKey(), rawEvent.getValue(), rawEvent.getClass().equals(startTypeClass) ? "start": rawEvent.getClass().equals(endTypeClass)? "end": "forbidden", rawEvent.getTimestamp());
            }
        });

        // Define the type info to avoid type info vanishing during transformations
        TupleTypeInfo<Tuple4<String, Double, String, Long>> inputTupleInfo = new TupleTypeInfo<>(
                Types.STRING(),
                Types.DOUBLE(),
                Types.STRING(),
                Types.LONG()
        );

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);//.getTableEnvironment(env);
        tableEnv.registerDataStream("RawEvents",
                streamAsTuple, "ID, val, eventType, eventTime.rowtime");

        String queryString= buildQueryString();
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
        queryResultAsStream.print();
        //(MapFunction<Tuple5<String, Timestamp, Timestamp, Double, String>, W>) tuple -> targetTypeClass.getDeclaredConstructor(long.class, long.class, double.class, String.class, String.class).newInstance(tuple.f1.getTime(), tuple.f2.getTime(), tuple.f3, tuple.f4, tuple.f0)
        return queryResultAsStream.map(new TupleToIntervalMapper<>(targetTypeClass)).returns(targetTypeClass);


    }

    private String buildQueryString() {

//        String skipStrategy = "AFTER MATCH SKIP PAST LAST ROW";
//
//
//
//        StringBuilder sqlQuery = new StringBuilder();
//
//        sqlQuery.append("Select ID, sts, ets, intervalValue,valueDescription, intvDuration from RawEvents Match_Recognize (\n");
//        sqlQuery.append("PARTITION BY ID\n");
//        sqlQuery.append("ORDER BY eventTime\n");
//        sqlQuery.append("MEASURES\n");
//        sqlQuery.append("A.ID AS id,\n");
//        sqlQuery.append("A.eventTime AS sts,\n");
//        sqlQuery.append("B.eventTime AS ets,\n");
//
//        //get the value
//
//
//        sqlQuery.append("0 As intervalValue,");
//
//
//
//        // value description
//        sqlQuery.append("' ' AS valueDescription,\n");
//
//        // just for testing purposes
//        sqlQuery.append("TIMESTAMPDIFF(SECOND, A.eventTime, B.eventTime) AS intvDuration \n");
//
//        //Skip strategy
//        sqlQuery.append(skipStrategy).append("\n");
//        // pattern
//        if (forbiddenStream != null){
//            sqlQuery.append("PATTERN (A !B C)\n")
//        } else // we put one or more
//        {
//
//            sqlQuery.append("PATTERN (A+ B)\n");
//        }
//
//        // define clause
//        sqlQuery.append("DEFINE\n");
//        String conditionString;
//        if (condition instanceof AbsoluteCondition ) {
//            conditionString = condition.toString().replace("!", "not")
//                    .replace("==", "=")
//                    .replace("!=", "<>")
//                    .replace("&&", " AND ")
//                    .replace("||", " OR ")
//                    .replace("Math.abs", "ABS")
//                    .replace("value", "val");
//
//            if (within == null)
//
//                sqlQuery.append(String.format("A as A.%s,\n", conditionString));
//            else
//            {
//
//                sqlQuery.append(String.format("A as (A.%s and LAST(A.val,1) IS NULL) OR ( A.%s AND TIMESTAMPDIFF(SECOND, LAST(A.eventTime,1), A.eventTime) <= %d),\n", conditionString,conditionString,(long)Math.floor(this.within.toMilliseconds()/1000)));
//            }
//
//
//        } else {
//
////            throw new NotImplementedException();
//            conditionString = condition.toString();
//            String startCondition;
//            String relativeCondition;
//            startCondition = conditionString.substring(0, conditionString.indexOf(" Relative"));
//            relativeCondition = conditionString.substring(conditionString.indexOf(" Relative ") + 10);
//            startCondition = startCondition.replace("!", "not")
//                    .replace("==", "=")
//                    .replace("!=", "<>")
//                    .replace("&&", " AND ")
//                    .replace("||", " OR ")
//                    .replace("Math.abs", "ABS")
//                    .replace("value", "A.val");
//
//            sqlQuery.append(String.format("A as (%s and LAST(A.val,1) IS NULL) OR ", startCondition));
//
//            relativeCondition = relativeCondition.replace("!", "not")
//                    .replace("==", "=")
//                    .replace("!=", "<>")
//                    .replace("&&", " AND ")
//                    .replace("||", " OR ")
//                    .replace("valueMath.abs", "ABS")
//                    .replace("value", "A.val")
//                    .replace("last", "LAST(A.val,1)")
//                    .replace("first", "FIRST(A.val)")
//                    .replace("avg", "AVG(A.val)")
//                    .replace("sum", "SUM(A.val)")
//                    .replace("min", "MIN(A.val)")
//                    .replace("max", "MAX(A.val)")
//            ;
//
//            RelativeCondition relCondition = (RelativeCondition) condition;
//            String parsedLHSCond="";
//            String parsedRHSCond="";
//            if (relCondition.getRelativeLHS() instanceof AbsoluteCondition)
//            {
//                parsedLHSCond = ((AbsoluteCondition) relCondition.getRelativeLHS()).parse(-1,-2,-3,-4,-5,-6,-7);
//                parsedLHSCond = parsedLHSCond.replace("Math.abs","ABS")
//                        .replace("-7.0", "A.val")
//                        .replace("-1.0","FIRST(A.val)")
//                        .replace("-2.0","LAST(A.val,1)")
//                        .replace("-3.0","MIN(A.val)")
//                        .replace("-4.0","MAX(A.val)")
//                        .replace("-5.0","SUM(A.val)")
//                        .replace("-6.0","COUNT(A.val)");
//
//                //System.out.println(parsedCond);
//            }
//
//            if (relCondition.getRelativeRHS() instanceof AbsoluteCondition)
//            {
//                parsedRHSCond = ((AbsoluteCondition) relCondition.getRelativeRHS()).parse(-1,-2,-3,-4,-5,-6,-7);
//                parsedRHSCond = parsedRHSCond.replace("Math.abs","ABS")
//                        .replace("-7.0", "A.val")
//                        .replace("-1.0","FIRST(A.val)")
//                        .replace("-2.0","LAST(A.val,1)")
//                        .replace("-3.0","MIN(A.val)")
//                        .replace("-4.0","MAX(A.val)")
//                        .replace("-5.0","SUM(A.val)")
//                        .replace("-6.0","COUNT(A.val)");
//
//            }
//            if (parsedLHSCond.length() > 0 && parsedRHSCond.length() > 0)
//            {
//                relativeCondition = parsedLHSCond + " " + relCondition.getRelativeOperator().toString()+ " " + parsedRHSCond;
//            }
//            else if (parsedLHSCond.length() > 0)
//            {
//                relativeCondition = parsedLHSCond + " " + relCondition.getRelativeOperator().toString() + " " + relCondition.getRelativeRHS().toString();
//            }
//            else if (parsedRHSCond.length() > 0 )
//            {
//                relativeCondition = relCondition.getRelativeLHS().toString() + " " + relCondition.getRelativeOperator().toString() + " " + parsedRHSCond;
//            }
//            if (within != null)
//                sqlQuery.append("(");
//
//            sqlQuery.append(String.format("(%s)", relativeCondition));
//
//            if (within != null) {
//                sqlQuery.append(String.format(" AND (TIMESTAMPDIFF(SECOND, LAST(A.eventTime,1), A.eventTime) <= %d)", (long)Math.floor(this.within.toMilliseconds()/1000)));
//
//            }
//            sqlQuery.append(",\n");
//        }
//        sqlQuery.append("B As true\n");
//
//        sqlQuery.append(")");
//
//        return sqlQuery.toString();
        return null;
    }

    public DataStream<W> runWithCEP() throws Exception {
        validate();
        Pattern<RawEvent, ?> pattern = Pattern.<RawEvent>begin("start").where(new SimpleCondition<RawEvent>() {
            @Override
            public boolean filter(RawEvent s) throws Exception {
                if (startStreamCondition == null)
                    return true;
                else {
                    ScriptEngineManager mgr = new ScriptEngineManager();
                    ScriptEngine engine = mgr.getEngineByName("JavaScript");
                    String conditionString = startStreamCondition.toString();


                    conditionString = conditionString.replace("value", Double.toString(s.getValue()));
                    return (boolean) engine.eval(conditionString);
                }
            }
        }).subtype(startTypeClass);


        if (forbiddenStream != null) {
            pattern = pattern.notFollowedBy("forbidden").subtype(forbiddenTypeClass);
        }
        pattern = pattern.followedBy("end").where(new RelativeIterativeCondition<>(endStreamCondition, RelativeIterativeCondition.ConditionContainer.Where)).subtype(endTypeClass);

        if (within != null) {
            pattern = pattern.within(within);
        }
        DataStream<RawEvent> all = prepareInputStreams();


        PatternStream<RawEvent> patternStream = CEP.pattern(all, pattern);

        targetStream = patternStream.select(new HeterogeneousIntervalElementsCollector<>(targetTypeClass), TypeInformation.of(targetTypeClass));

        return targetStream;
    }

    private DataStream<RawEvent> prepareInputStreams() {
        // Partition the streams by key and use their RawEvent representation
        DataStream<RawEvent> startStreamAsRawEvent = startStream.keyBy((KeySelector<S, String>) RawEvent::getKey).map((MapFunction<S, RawEvent>) s -> s);

        DataStream<RawEvent> endStreamAsRawEvent = endStream.keyBy((KeySelector<E, String>) RawEvent::getKey).map((MapFunction<E, RawEvent>) s -> s);
        DataStream<RawEvent> forbiddenStreamAsRawEvent;
        DataStream<RawEvent> all = startStreamAsRawEvent.union(endStreamAsRawEvent);
        if (forbiddenStream != null) {
            forbiddenStreamAsRawEvent = forbiddenStream.keyBy((KeySelector<F, String>) RawEvent::getKey).map((MapFunction<F, RawEvent>) f -> f);
            all = all.union(forbiddenStreamAsRawEvent);
        }
        return all;
    }


}
