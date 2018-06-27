package tests;

import io.flinkspector.datastream.DataStreamTestEnvironment;
import io.flinkspector.datastream.input.EventTimeInput;
import io.flinkspector.datastream.input.EventTimeInputBuilder;
import io.flinkspector.datastream.input.time.After;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

public class RedundandTest {


    @Test
    public void windowFunctionTest() throws Exception {
        DataStreamTestEnvironment env = DataStreamTestEnvironment.createTestEnvironment(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        //specify input
        EventTimeInput<Tuple2<String, String>> input = EventTimeInputBuilder.startWith(Tuple2.of("whatever, this is timestamp", "B"))
                .emit(Tuple2.of("54543", "B"), After.period(2, TimeUnit.SECONDS))
                .emit(Tuple2.of("jaka1234zina", "B"), After.period(2, TimeUnit.SECONDS))
                .emit(Tuple2.of("j4312a", "B"), After.period(2, TimeUnit.SECONDS))
                .emit(Tuple2.of("jak431235 godzina", "B"), After.period(2, TimeUnit.SECONDS))
                .emit(Tuple2.of("54543", "C"), After.period(2, TimeUnit.SECONDS))
                .emit(Tuple2.of("54543", "C"), After.period(2, TimeUnit.SECONDS))
                .emit(Tuple2.of("54543", "C"), After.period(2, TimeUnit.SECONDS))
                .emit(Tuple2.of("godzina pogańska", "A"), After.period(2, TimeUnit.SECONDS))
                .emit(Tuple2.of("go345543ńska", "A"), After.period(2, TimeUnit.SECONDS));


        //acquire data source from input
        DataStream<Tuple2<String, String>> stream = env.fromInput(input);
        //apply transformation
        stream.flatMap(new Transformator())//keyowanie chyba mi rozwala strumień na równoległe sturmienie z krotką w każdym
                .keyBy(0)
                .timeWindow(Time.seconds(50))//on jak zrobi keyby to jedzie na oddzielnych stroumieniach, jeżeli użyję takiego rozwiązania to musze te strumienie zebrać jakoś spowrotem
                .reduce(new MyReduceFunction())
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(50)))
                .apply(new AllWindowFunction<Tuple2<String,Integer>, Object, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<Tuple2<String, Integer>> iterable, Collector<Object> collector) throws Exception {
                        ArrayList<Tuple2<String, Integer>> temp = new ArrayList<>();
                        Iterator iterator = iterable.iterator();
                        while (iterator.hasNext())
                            temp.add((Tuple2<String, Integer>) iterator.next());
                        Collections.sort(temp, new CustomComparator());
                        for (int i = 0; i < temp.size(); i++) {
                            collector.collect(temp.get(i));
                        }
                    }
                })
                .print();


        env.execute();

    }

    private class Transformator implements FlatMapFunction<Tuple2<String, String>, Tuple2<String, Integer>> {
        @Override
        public void flatMap(Tuple2<String, String> inputTuple, Collector<Tuple2<String, Integer>> collector) throws Exception {
            collector.collect(new Tuple2<>(inputTuple.f1, 1));
        }
    }


    private static class MyReduceFunction implements ReduceFunction<Tuple2<String, Integer>> {

        public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) {
            return t1.f0.equals(t2.f0) ? new Tuple2<>(t1.f0, t1.f1 + t2.f1) : t1;
        }
    }



    private static class CustomComparator implements java.util.Comparator<Tuple2<String, Integer>> {
        @Override
        public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
            return o1.f1 > o2.f1 ? -1 : (o1.f1 < o2.f1 ? 1 : 0);
        }
    }

}






