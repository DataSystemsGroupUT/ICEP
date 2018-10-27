package ut.ee.icep.generator;

import bsh.Interpreter;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.types.StringValue;

import java.io.Serializable;
import java.util.ArrayList;
import ut.ee.icep.events.*;

public class IntervalGeneratorHetrogenous < Z extends RawEvent, S extends RawEvent,W extends IntervalEvent> implements Serializable{

    private Class<S> sourceTypeClass;
    private Class<Z> sourceTypeClass2;

    private Class<W> targetTypeClass;


    DataStream<S> sourceStream;
    DataStream<W> targetStream;
    DataStream<Z> sourceStream2;


    int minOccurs;
    int maxOccurs;
    IntervalGeneratorHetrogenous.Operator operator;
    static double compareValue;
    Time  timee;
    public static final String VALUE = "tempValue";
IntervalGenerator.Operator outvalue;

    public enum Operator
    {
        Equals,
        LessThan,
        LessThanEqual,
        GreaterThan,
        GreaterThanEqual,
        NotEqual
    }

    public IntervalGeneratorHetrogenous sourceType(Class<Z> srctype , Class<S> sourceTyp)
    {
        this.sourceTypeClass = sourceTyp;
        this.sourceTypeClass2 = srctype;
        return this;
    }

    public IntervalGeneratorHetrogenous source(DataStream<Z> srstream , DataStream<S> srcStream)
    {
        this.sourceStream = srcStream;
        this.sourceStream2 = srstream;
        return this;
    }

    public IntervalGeneratorHetrogenous target(DataStream<W> trgtStream)
    {
        this.targetStream = trgtStream;
        return this;
    }

    public IntervalGeneratorHetrogenous targetType(Class<W> targetTyp)
    {
        this.targetTypeClass = targetTyp;
        return this;
    }


    public IntervalGeneratorHetrogenous condition(int min, int max, IntervalGeneratorHetrogenous.Operator op, double value , Time te)
    {
        this.minOccurs = min;
        this.maxOccurs = max;
        this.operator = op;
        this.timee = te;
        this.compareValue = value;
        return this;
    }

    public String enumInIff(IntervalGeneratorHetrogenous.Operator opp){
        if (operator == IntervalGeneratorHetrogenous.Operator.GreaterThan){
            return ">";
        }
        else if (operator == IntervalGeneratorHetrogenous.Operator.Equals){
            return "==";
        }
        else if (operator == IntervalGeneratorHetrogenous.Operator.LessThan){
            return "<";
        }
        else if (operator == IntervalGeneratorHetrogenous.Operator.LessThanEqual){
            return "<=";
        }
        else if (operator == IntervalGeneratorHetrogenous.Operator.GreaterThanEqual){
            return ">=";
        }
        else if (operator == IntervalGeneratorHetrogenous.Operator.NotEqual){
            return "!=";
        }
        else {
            return "unvalid operator";
        }
    }

    /**

     */

    public DataStream<W> run_generator() {
        String condition = (IntervalGenerator.VALUE + enumInIff(operator) + compareValue);

        Pattern<Z, ?>interval = Pattern.<Z>begin("1")
                .subtype(sourceTypeClass2)
                .where(new IterativeCondition<Z>() {
                    private static final long serialVersionUID = 2392863109523984059L;

                    @Override
                    public boolean filter(Z value, IterativeCondition.Context<Z> ctx) throws Exception {

                        if (value.getValue() > compareValue) {
                            double val = value.getValue();
                            Interpreter interpreter = new Interpreter();
                            interpreter.set(VALUE, val);
                            boolean result = (boolean) interpreter.eval(condition);
                            return result;
                        }
                        return false;
                    }
                });
        /**

         */
        for (int i = minOccurs+1; i <= maxOccurs; i++){
            String conditionn = IntervalGenerator.VALUE +enumInIff(operator);

            int finalI = i;
            interval = interval.followedBy(Integer.toString(i))
                    .subtype(sourceTypeClass2)
                    .where(( IterativeCondition<Z> ) new IterativeCondition<S>(){
                        private int index;
                        {
                            index = finalI;
                        }
                        @Override
                        public boolean filter(S value, Context<S> ctx) throws Exception{


                            double val = 0;
                            for (S event : ctx.getEventsForPattern(Integer.toString(index-1))){
                                val = event.getValue();
                            }
                            if (value.getValue() >= val){
                                Interpreter interpreter2 = new Interpreter();
                                interpreter2.set(VALUE, val);
                                boolean result = (boolean) interpreter2.eval(conditionn + val);
                                return result;

                            }else
                                return false;
                        }
                    }).within( timee );
        }
        PatternStream<S> pattern = ( PatternStream<S> ) CEP.pattern(sourceStream2.keyBy(new KeySelector<Z, String>(){
            @Override
            public String getKey(Z value) throws Exception{
                return  value.getKey();
            }
        }), interval);
        targetStream =  pattern.select(new ElementsCollector<S, W>(targetTypeClass, minOccurs, maxOccurs,0, outvalue),TypeInformation.of(targetTypeClass));
        return targetStream;
    }

    public DataStream<W> run_loop_generator(int times) {
        String condition = (IntervalGenerator.VALUE + enumInIff(operator) + compareValue);

        Pattern< S, ?>interval2 = Pattern.<S>begin("1").times(times)
                .subtype(sourceTypeClass)
                .where(new IterativeCondition<S>() {
                    private static final long serialVersionUID = 2392863109523984059L;

                    @Override
                    public boolean filter(S value, IterativeCondition.Context<S> ctx) throws Exception {

                        if (value.getValue() > compareValue) {
                            double val = value.getValue();
                            Interpreter interpreter = new Interpreter();
                            interpreter.set(VALUE, val);
                            boolean result = (boolean) interpreter.eval(condition);
                            return result;
                        }
                        return false;
                    }
                }).within(timee);
        /**

         */
        PatternStream<S> pattern = CEP.pattern(sourceStream.keyBy(new KeySelector<S, String>(){
            @Override
            public String getKey(S value) throws Exception{
                return  value.getKey();
            }
        }), interval2);
        targetStream =  pattern.select(new ElementsCollector<S, W>(targetTypeClass, minOccurs, maxOccurs,1, outvalue), TypeInformation.of(targetTypeClass));
        return targetStream;
    }

}
