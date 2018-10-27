/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ut.ee.icep.generator;

/**
 *
 * @author MKamel
 */

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

import javax.management.AttributeList;


public class IntervalGenerator <S extends RawEvent,W extends IntervalEvent> implements Serializable{
    
    private Class<S> sourceTypeClass;
	private Class<W> targetTypeClass;
	DataStream<S> sourceStream;
	DataStream<W> targetStream;
	int minOccurs;
	 int maxOccurs;
	Operator operator;

	Operator outvalue;
static 	double val = 0;

	String contype;
	static double sum =0 ;
	static double avgg =0 ;

	static ArrayList<Double> places = new ArrayList<Double>();




	static double compareValue;
	Time  timee;
	public static final String VALUE = "tempValue";
	private static ArrayList<Integer> eventIndecies = new ArrayList();


	public enum Operator
	{
		Equals,
		LessThan,
		LessThanEqual,
		GreaterThan,
		GreaterThanEqual,
		NotEqual,
		First,
		Last,
		Average,
		Sum
	}

	public IntervalGenerator sourceType(Class<S> sourceTyp)
	{
		this.sourceTypeClass = sourceTyp;
		return this;
	}

	public IntervalGenerator source(DataStream<S> srcStream)
	{
		this.sourceStream = srcStream;
		return this;
	}

	public IntervalGenerator target(DataStream<W> trgtStream)
	{
		this.targetStream = trgtStream;
		return this;
	}
	public IntervalGenerator targetType(Class<W> targetTyp)
	{
		this.targetTypeClass = targetTyp;
		return this;
	}


	public IntervalGenerator condition(int min, int max,  Operator op, double value , Time te)
	{
		this.minOccurs = min;
		this.maxOccurs = max;
		this.operator = op;
		this.timee = te;
		this.compareValue = value;
		return this;
	}

	public IntervalGenerator outvalue(Operator outvalue)
	{
		this.outvalue = outvalue;
		return this;
	}

	public IntervalGenerator conditionType(String  conditiontype)
	{
		this.contype = conditiontype;
		return this;
	}

	public String enumInIff(Operator opp){
		if (operator == Operator.GreaterThan){
			return ">";
		}
		else if (operator == Operator.Equals){
			return "==";
		}
		else if (operator == Operator.LessThan){
			return "<";
		}
		else if (operator == Operator.LessThanEqual){
			return "<=";
		}
		else if (operator == Operator.GreaterThanEqual){
			return ">=";
		}
		else if (operator == Operator.NotEqual){
			return "!=";
		}
		else if (operator == Operator.First){
			return "First";
		}
		else if (operator == Operator.Last){
			return "Last";
		}
		else if (operator == Operator.Average){
			return "Average";
		}else if (operator == Operator.Sum){
			return "Sum";
		}
		else {
			return "unvalid operator";
		}
	}

     /**

	 */



	public DataStream<W> run_generator() {
	//	ElementsCollector ee = new ElementsCollector();


		if (contype == "A") {

			String condition = (IntervalGenerator.VALUE + enumInIff(operator) + compareValue);
			Pattern<S, ?> interval = Pattern.<S>begin("1")
					.subtype(sourceTypeClass)
					.where(new IterativeCondition<S>() {
						private static final long serialVersionUID = 2392863109523984059L;

						@Override
						public boolean filter(S value, IterativeCondition.Context<S> ctx) throws Exception {

							if (value.getValue() > compareValue) {
								double val = value.getValue();
								Interpreter interpreter = new Interpreter();
								interpreter.set(VALUE, val);
//								System.out.println("Value = TempValue = " + val);
								boolean result = ( boolean ) interpreter.eval(condition);
//								System.out.println("condition = " + condition);

								return result;
							}
							return false;
						}
					});
			/**

			 */
			for (int i = minOccurs + 1; i <= maxOccurs; i++) {
				//String conditionn = (IntervalGenerator.VALUE + enumInIff(operator) + compareValue);

				int finalI = i;
				interval = interval.followedBy(Integer.toString(i))
						.subtype(sourceTypeClass)
						.where(new IterativeCondition<S>() {
							private int index;

							{
								index = finalI;
							}

							@Override
							public boolean filter(S value, Context<S> ctx) throws Exception {


								double vall = 0;
//								for (S event : ctx.getEventsForPattern(Integer.toString(index - 1))) {
//									vall = event.getValue();
//								}

								if (value.getValue() > compareValue) {
									double newval = value.getValue();
									Interpreter interpreter2 = new Interpreter();
									interpreter2.set(VALUE, newval);
									boolean result = ( boolean ) interpreter2.eval(condition);
									return result;

								} else
									return false;
							}
						}).within(timee);
			}


			PatternStream<S> pattern = CEP.pattern(sourceStream.keyBy(new KeySelector<S, String>() {
				@Override
				public String getKey(S value) throws Exception {
					String i = String.valueOf(value.getRackID());
					return i;

				}
			}), interval);
			targetStream = pattern.select(new ElementsCollector<S, W>(targetTypeClass, minOccurs, maxOccurs, 0, outvalue), TypeInformation.of(targetTypeClass));

			////   FOR RELATIVE CONDITION

		}else if (contype == "R"){
				String condition = (IntervalGenerator.VALUE + enumInIff(operator) + compareValue);

				Pattern< S, ?>interval = Pattern.<S>begin("1")
						.subtype(sourceTypeClass)
						.where(new IterativeCondition<S>() {
							private static final long serialVersionUID = 2392863109523984059L;

							@Override
							public boolean filter(S value, IterativeCondition.Context<S> ctx) throws Exception {

								if (value.getValue() > compareValue ) {
									double valll = value.getValue();
									//sum = 0;

									Interpreter interpreter = new Interpreter();
									interpreter.set(VALUE, valll);
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
							.subtype(sourceTypeClass)
							.where(new IterativeCondition<S>(){
								private int index;
								{
									index = finalI;
								}
								@Override
								public boolean filter(S value, Context<S> ctx) throws Exception{

									for (S event : ctx.getEventsForPattern(Integer.toString(index-1))){
										Integer inx = index - 1;
										boolean exist = false;
										for(Integer i : eventIndecies) {
											if (i == inx) {
												exist = true;
											}
										}
										if(!exist) {
											eventIndecies.add(inx);
											sum += event.getValue();
										}
										val = event.getValue();
//
									}
									//sum = value.getValue();
									/*
									for (S event : ctx.getEventsForPattern(Integer.toString(index-1))){
										val = event.getValue();
//										sum += event.getValue();
									}*/
									System.out.println("Sum = "+  sum + " Index = "+ (index - 1) + "  " );


//									double avg = sum /(index) ;

									if (value.getValue() > val ){
										double newval = value.getValue()  ;
										Interpreter interpreter2 = new Interpreter();
										interpreter2.set(VALUE, newval);
										boolean result = (boolean) interpreter2.eval(conditionn + val);
//										System.out.println("condition = " + conditionn + val);
										return result;

									}else
										return false;
								}
							}).within( timee );

			}

			PatternStream<S> pattern = CEP.pattern(sourceStream.keyBy(new KeySelector<S, String>() {
				@Override
				public String getKey(S value) throws Exception {
					String i = String.valueOf(value.getRackID());
					return i;

				}
			}), interval);
			targetStream = pattern.select(new ElementsCollector<S, W>( targetTypeClass, minOccurs, maxOccurs, 0, outvalue), TypeInformation.of(targetTypeClass));
		}
		return targetStream;
	}

//	public static void  clearIndecies() {
//		eventIndecies.clear();
//	}

	public DataStream<W> run_loop_generator(int times) {
			String condition = (IntervalGenerator.VALUE + enumInIff(operator) + compareValue);

			Pattern<S, ?> interval2 = Pattern.<S>begin("1").times(times)
					.subtype(sourceTypeClass)
					.where(new IterativeCondition<S>() {
						private static final long serialVersionUID = 2392863109523984059L;

						@Override
						public boolean filter(S value, IterativeCondition.Context<S> ctx) throws Exception {

							if (value.getValue() > compareValue) {
								double val = value.getValue();
								Interpreter interpreter = new Interpreter();
								interpreter.set(VALUE, val);
								boolean result = ( boolean ) interpreter.eval(condition);
								return result;
							}
							return false;
						}
					}).within(timee);
			/**

			 */
			PatternStream<S> pattern = CEP.pattern(sourceStream.keyBy(new KeySelector<S, String>() {
				@Override
				public String getKey(S value) throws Exception {
					String i = String.valueOf(value.getRackID());

					return i;
				}
			}), interval2);
			targetStream = pattern.select(new ElementsCollector<S, W>(targetTypeClass, minOccurs, maxOccurs, 1, outvalue, times), TypeInformation.of(targetTypeClass));

		return targetStream;
	}
    
}
