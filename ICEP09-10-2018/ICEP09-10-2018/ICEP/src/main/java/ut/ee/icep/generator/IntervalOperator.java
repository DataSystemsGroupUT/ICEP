/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package ut.ee.icep.generator;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import ut.ee.icep.events.IntervalEvent;

/**
 *
 * @author MKamel
 */
public class IntervalOperator {

    public static <X extends IntervalEvent> DataStream<?> before(DataStream<X> stream1, DataStream<X> stream2, Time te) {
        DataStream<?> joinedStream = stream1.join(stream2)
                .where(
                        new KeySelector<X, String>() {
                            @Override
                            public String getKey(X value) throws Exception {
                                String k = String.valueOf(value.getRackID());
                                return k;
                            }
                        })
                .equalTo(new KeySelector<X, String>() {
                    @Override
                    public String getKey(X value) throws Exception {
                        String k = String.valueOf(value.getRackID());
//                        System.out.println("K = " + k);
                        return k;
                    }
                })
                .window(TumblingEventTimeWindows.of(te))
                .apply(new JoinFunction<X, X, Match>() {
                    @Override
                    public Match join(X first, X second) throws Exception {
                        if ((first.getStartTimestamp() < second.getStartTimestamp()) && (first.getEndTimestamp() < second.getStartTimestamp())
                        ) {
                            Match match = new Match();
                            match.e1 = first;
                            match.e2 = second;
                            match.op = "Before";
                            match.rid= first.getRackID();
//                            System.out.println(first.getRackID() + "" + second.getRackID());
                            return match;
                        } else {
                            return new Match();
                        }
                    }
                });
        return joinedStream;
    }

    public static <X extends IntervalEvent> DataStream<?> meets(DataStream<X> stream1, DataStream<X> stream2, Time te) {
        DataStream<?> joinedStream = stream1.join(stream2)
                .where(
                        new KeySelector<X, String>() {
                            @Override
                            public String getKey(X value) throws Exception {
                                String k = String.valueOf(value.getRackID());
                                return k;
                            }
                        })
                .equalTo(new KeySelector<X, String>() {
                    @Override
                    public String getKey(X value) throws Exception {
                        String k = String.valueOf(value.getRackID());
                        return k;
                    }
                })
                .window(TumblingEventTimeWindows.of(te))
                .apply(new JoinFunction<X, X, Match>() {
                    @Override
                    public Match join(X first, X second) throws Exception {
                        if ((first.getStartTimestamp() < second.getStartTimestamp()) && (first.getEndTimestamp() == second.getStartTimestamp())) {
                            Match match = new Match();
                            match.e1 = first;
                            match.e2 = second;
                            match.op = "Meets";
                            match.rid= first.getRackID();
                            return match;
                        } else {
                            return new Match();						}
                    }
                });
        return joinedStream;
    }

    public static <X extends IntervalEvent> DataStream<?> equalTo(DataStream<X> stream1, DataStream<X> stream2, Time te) {
        DataStream<?> joinedStream = stream1.join(stream2)
                .where(
                        new KeySelector<X, String>() {
                            @Override
                            public String getKey(X value) throws Exception {
                                String k = String.valueOf(value.getRackID());
                                return k;
                            }
                        })
                .equalTo(new KeySelector<X, String>() {
                    @Override
                    public String getKey(X value) throws Exception {
                        String k = String.valueOf(value.getRackID());
                        return k;
                    }
                })
                .window(TumblingEventTimeWindows.of(te))
                .apply(new JoinFunction<X, X, Match>() {
                    @Override
                    public Match join(X first, X second) throws Exception {
                        if ((first.getStartTimestamp() == second.getStartTimestamp()) && (first.getEndTimestamp() == second.getEndTimestamp())){
                            Match match = new Match();
                            match.e1 = first;
                            match.e2 = second;
                            match.op= "EqualTo";
                            match.rid= first.getRackID();
                            return match;
                        } else {
                            return new Match();						}
                    }
                });
        return joinedStream;
    }

    public static <X extends IntervalEvent> DataStream<?> overlap(DataStream<X> stream1, DataStream<X> stream2, Time te) {
        DataStream<?> joinedStream = stream1.join(stream2)
                .where(
                        new KeySelector<X, String>() {
                            @Override
                            public String getKey(X value) throws Exception {
                                String k = String.valueOf(value.getRackID());
                                return k;
                            }
                        })
                .equalTo(new KeySelector<X, String>() {
                    @Override
                    public String getKey(X value) throws Exception {
                        String k = String.valueOf(value.getRackID());
                        return k;
                    }
                })
                .window(TumblingEventTimeWindows.of(te))
                .apply(new JoinFunction<X, X, Match>() {
                    @Override
                    public Match join(X first, X second) throws Exception {
                        if ((first.getStartTimestamp() < second.getStartTimestamp()) && (first.getEndTimestamp() > second.getStartTimestamp())
                                && first.getEndTimestamp() < second.getEndTimestamp()){
                            Match match = new Match();
                            match.e1 = first;
                            match.e2 = second;
                            match.op= "Overlap";
                            match.rid= first.getRackID();
                            return match;
                        } else {
                            return new Match();						}
                    }
                });
        return joinedStream;
    }

    public static  <X extends IntervalEvent> DataStream<?> during(DataStream<X> stream1, DataStream<X> stream2, Time te) {
        DataStream<?> joinedStream = stream1.join(stream2)
                .where(
                        new KeySelector<X, String>() {
                            @Override
                            public String getKey(X value) throws Exception {
                                String k = String.valueOf(value.getRackID());
                                return k;
                            }
                        })
                .equalTo(new KeySelector<X, String>() {
                    @Override
                    public String getKey(X value) throws Exception {
                        String k = String.valueOf(value.getRackID());
                        return k;
                    }
                })
                .window(TumblingEventTimeWindows.of(te))
                .apply(new JoinFunction<X, X, Match>() {
                    @Override
                    public Match join(X first, X second) throws Exception {
                        if ((first.getStartTimestamp() > second.getStartTimestamp()) && (first.getEndTimestamp() < second.getEndTimestamp()) ){
                            Match match = new Match();
                            match.e1 = first;
                            match.e2 = second;
                            match.op= "During";
                            match.rid= first.getRackID();

                            return match;
                        } else {
                            return new Match();						}
                    }
                });
        return joinedStream;
    }

    public static  <X extends IntervalEvent> DataStream<?> starts(DataStream<X> stream1, DataStream<X> stream2, Time te) {
        DataStream<?> joinedStream = stream1.join(stream2)
                .where(
                        new KeySelector<X, String>() {
                            @Override
                            public String getKey(X value) throws Exception {
                                String k = String.valueOf(value.getRackID());
                                return k;
                            }
                        })
                .equalTo(new KeySelector<X, String>() {
                    @Override
                    public String getKey(X value) throws Exception {
                        String k = String.valueOf(value.getRackID());
                        return k;
                    }
                })
                .window(TumblingEventTimeWindows.of(te))
                .apply(new JoinFunction<X, X, Match>() {
                    @Override
                    public Match join(X first, X second) throws Exception {
                        if ((first.getStartTimestamp() == second.getStartTimestamp()) && (first.getEndTimestamp() < second.getEndTimestamp()) ){
                            Match match = new Match();
                            match.e1 = first;
                            match.e2 = second;
                            match.op= "Starts";
                            match.rid= first.getRackID();

                            return match;
                        } else {
                            return new Match();						}
                    }
                });
        return joinedStream;
    }

    public static  <X extends IntervalEvent> DataStream<?> finishes(DataStream<X> stream1, DataStream<X> stream2, Time te) {
        DataStream<?> joinedStream = stream1.join(stream2)
                .where(
                        new KeySelector<X, String>() {
                            @Override
                            public String getKey(X value) throws Exception {
                                String k = String.valueOf(value.getRackID());
                                return k;
                            }
                        })
                .equalTo(new KeySelector<X, String>() {
                    @Override
                    public String getKey(X value) throws Exception {
                        String k = String.valueOf(value.getRackID());
                        return k;
                    }
                })
                .window(TumblingEventTimeWindows.of(te))
                .apply(new JoinFunction<X, X, Match>() {
                    @Override
                    public Match join(X first, X second) throws Exception {
                        if ((first.getStartTimestamp() > second.getStartTimestamp()) && (first.getEndTimestamp() == second.getEndTimestamp()) ){
                            Match match = new Match();
                            match.e1 = first;
                            match.e2 = second;
                            match.op= "Finishes";
                            match.rid= first.getRackID();

                            return match;
                        } else {
                            return new Match();						}
                    }
                });
        return joinedStream;
    }

    public static  <X extends IntervalEvent> DataStream<?> contains(DataStream<X> stream1, DataStream<X> stream2, Time te) {
        DataStream<?> joinedStream = stream1.join(stream2)
                .where(
                        new KeySelector<X, String>() {
                            @Override
                            public String getKey(X value) throws Exception {
                                String k = String.valueOf(value.getRackID());
                                return k;
                            }
                        })
                .equalTo(new KeySelector<X, String>() {
                    @Override
                    public String getKey(X value) throws Exception {
                        String k = String.valueOf(value.getRackID());
                        return k;
                    }
                })
                .window(TumblingEventTimeWindows.of(te))
                .apply(new JoinFunction<X, X, Match>() {
                    @Override
                    public Match join(X first, X second) throws Exception {
                        if ((first.getStartTimestamp() < second.getStartTimestamp()) && (first.getEndTimestamp() > second.getEndTimestamp()) ){
                            Match match = new Match();
                            match.e1 = first;
                            match.e2 = second;
                            match.op= "Contains";
                            match.rid= first.getRackID();

                            return match;
                        } else {
                            return new Match();						}
                    }
                });
        return joinedStream;
    }

    public static  <X extends IntervalEvent> DataStream<?> startsBy(DataStream<X> stream1, DataStream<X> stream2, Time te) {
        DataStream<?> joinedStream = stream1.join(stream2)
                .where(
                        new KeySelector<X, String>() {
                            @Override
                            public String getKey(X value) throws Exception {
                                String k = String.valueOf(value.getRackID());
                                return k;
                            }
                        })
                .equalTo(new KeySelector<X, String>() {
                    @Override
                    public String getKey(X value) throws Exception {
                        String k = String.valueOf(value.getRackID());
                        return k;
                    }
                })
                .window(TumblingEventTimeWindows.of(te))
                .apply(new JoinFunction<X, X, Match>() {
                    @Override
                    public Match join(X first, X second) throws Exception {
                        if ((first.getStartTimestamp() == second.getStartTimestamp()) && (first.getEndTimestamp() > second.getEndTimestamp()) ){
                            Match match = new Match();
                            match.e1 = first;
                            match.e2 = second;
                            match.op= "StartsBy";
                            match.rid= first.getRackID();

                            return match;
                        } else {
                            return new Match();						}
                    }
                });
        return joinedStream;
    }

    public static  <X extends IntervalEvent> DataStream<?> overlapby(DataStream<X> stream1, DataStream<X> stream2, Time te) {
        DataStream<?> joinedStream = stream1.join(stream2)
                .where(
                        new KeySelector<X, String>() {
                            @Override
                            public String getKey(X value) throws Exception {
                                String k = String.valueOf(value.getRackID());
                                return k;
                            }
                        })
                .equalTo(new KeySelector<X, String>() {
                    @Override
                    public String getKey(X value) throws Exception {
                        String k = String.valueOf(value.getRackID());
                        return k;
                    }
                })
                .window(TumblingEventTimeWindows.of(te))
                .apply(new JoinFunction<X, X, Match>() {
                    @Override
                    public Match join(X first, X second) throws Exception {
                        if ((first.getStartTimestamp() > second.getStartTimestamp()) && (first.getEndTimestamp() > second.getEndTimestamp()) ){
                            Match match = new Match();
                            match.e1 = first;
                            match.e2 = second;
                            match.op= "OverlapBy";
                            match.rid= first.getRackID();

                            return match;
                        } else {
                            return new Match();						}
                    }
                });
        return joinedStream;
    }

    public static  <X extends IntervalEvent> DataStream<?> metBy(DataStream<X> stream1, DataStream<X> stream2, Time te) {
        DataStream<?> joinedStream = stream1.join(stream2)
                .where(
                        new KeySelector<X, String>() {
                            @Override
                            public String getKey(X value) throws Exception {
                                String k = String.valueOf(value.getRackID());
                                return k;
                            }
                        })
                .equalTo(new KeySelector<X, String>() {
                    @Override
                    public String getKey(X value) throws Exception {
                        String k = String.valueOf(value.getRackID());
                        return k;
                    }
                })
                .window(TumblingEventTimeWindows.of(te))
                .apply(new JoinFunction<X, X, Match>() {
                    @Override
                    public Match join(X first, X second) throws Exception {
                        if ((second.getStartTimestamp() < first.getStartTimestamp()) && (first.getStartTimestamp() == second.getEndTimestamp())  && (first.getEndTimestamp() > second.getEndTimestamp())){
                            Match match = new Match();
                            match.e1 = first;
                            match.e2 = second;
                            match.op= "MetBy";
                            match.rid= first.getRackID();

                            return match;
                        } else {
                            return new Match();						}
                    }
                });
        return joinedStream;
    }

    public static  <X extends IntervalEvent> DataStream<?> after(DataStream<X> stream1, DataStream<X> stream2, Time te) {
        DataStream<?> joinedStream = stream1.join(stream2)
                .where(
                        new KeySelector<X, String>() {
                            @Override
                            public String getKey(X value) throws Exception {
                                String k = String.valueOf(value.getRackID());
                                return k;
                            }
                        })
                .equalTo(new KeySelector<X, String>() {
                    @Override
                    public String getKey(X value) throws Exception {
                        String k = String.valueOf(value.getRackID());
                        return k;
                    }
                })
                .window(TumblingEventTimeWindows.of(te))
                .apply(new JoinFunction<X, X, Match>() {
                    @Override
                    public Match join(X first, X second) throws Exception {
                        if ((second.getStartTimestamp() < first.getStartTimestamp()) && (second.getEndTimestamp() < first.getStartTimestamp()) ){
                            Match match = new Match();
                            match.e1 = first;
                            match.e2 = second;
                            match.op= "After";
                            match.rid= first.getRackID();

                            return match;
                        } else {
                            return new Match();						}
                    }
                });
        return joinedStream;
    }
}
