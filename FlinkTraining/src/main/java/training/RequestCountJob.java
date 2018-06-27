package training;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import org.apache.sling.commons.json.JSONObject;
import java.util.*;


/**
 * Skeleton for a Flink Streaming Job.
 *
 * For a full example of a Flink Streaming Job, see the SocketTextStreamWordCount.java
 * file in the same package/directory or have a look at the website.
 *
 * You can also generate a .jar file that you can submit on your Flink
 * cluster.
 * Just type
 * 		mvn clean package
 * in the projects root directory.
 * You will find the jar in
 * 		target/FlinkTraining-1.0.jar
 * From the CLI you can then run
 * 		./bin/flink run -c training.RequestCountJob target/FlinkTraining-1.0.jar
 *
 * For more information on the CLI see:
 *
 * http://flink.apache.org/docs/latest/apis/cli.html
 */
public class RequestCountJob {

	public static final int WINDOW_TIME = 50;

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment

        final ParameterTool params = ParameterTool.fromArgs(args);
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		//get input data by connecting to the socket
		DataStreamSource<String> dataStreamSource = env.addSource(new FlinkKafkaConsumer010<>(
		        params.getRequired("input-topic-name"),
                new SimpleStringSchema(),
                properties));
		DataStream<String> streamTuples = dataStreamSource
                .flatMap(new Splitter())
				.windowAll(TumblingEventTimeWindows.of(Time.seconds(WINDOW_TIME)))
				.apply(new AllWindowFunction<String, String, TimeWindow>() {
					@Override
					public void apply(TimeWindow timeWindow, Iterable<String> iterable, Collector<String> collector) throws Exception {
						Map<String, Integer> temp = new HashMap<>();
						Iterator iterator = iterable.iterator();
						while (iterator.hasNext()) {
							String elem = (String) iterator.next();
							if (!temp.containsKey(elem))	{
								temp.put(elem, 1);
							}
							else {
								temp.put(elem, temp.get(elem) + 1);
							}
						}
						ArrayList<Tuple2<String, Integer>> temp2 = new ArrayList<>();
						for (String key: temp.keySet())	{
							temp2.add(new Tuple2<>(key, temp.get(key)));
						}
						Collections.sort(temp2, new CustomComparator());
						JSONObject result = new JSONObject();

						for (int i = 0; i < temp2.size(); i++) {
							result.append(temp2.get(i).f0, temp2.get(i).f1.toString());
						}
						collector.collect(result.toString());
					}
				});
		streamTuples.addSink(
				new FlinkKafkaProducer010<>(
						params.getRequired("output-topic-name"),
						new SimpleStringSchema(),
						properties));


        streamTuples.print();
		// execute program
		env.execute("Flink Streaming Java API User Request Job");
	}

	public static class Splitter implements FlatMapFunction<String, String> {
        @Override
        public void flatMap(String s, Collector<String> collector) throws Exception {
            JSONObject jsonObject = new JSONObject(s);
            collector.collect(jsonObject.getString("user"));
        }
    }


//	private static class MyReduceFunction implements ReduceFunction<Tuple2<String, Integer>> {
//
//		public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) {
//			return t1.f0.equals(t2.f0) ? new Tuple2<>(t1.f0, t1.f1+t2.f1) : t1;
//		}
//	}
//
//	private static class MyWindowFunction
//			implements WindowFunction<Tuple2<String, Integer>, String, Tuple, TimeWindow> {
//
//
//		//TODO gówno nie działa i przyczyna tego znajduje się gdzieś tu poniżej
//		@Override
//		public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple2<String, Integer>> iterable, Collector<String> collector) throws Exception {
//			ArrayList<Tuple2<String, Integer>> temp = new ArrayList<>();
//			Iterator iterator = iterable.iterator();
//			while (iterator.hasNext())
//				temp.add((Tuple2<String, Integer>) iterator.next());
//			Collections.sort(temp, new CustomComparator());
//			for (int i = 0;i<temp.size();i++)
//				collector.collect("{\"" + temp.get(i).f0.toString() + "\": " + "\""+temp.get(i).f1.toString()+"\"}");
//		}
//	}

		private static class CustomComparator implements java.util.Comparator<Tuple2<String, Integer>> {
			@Override
			public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
				return o1.f1>o2.f1 ? 1 : -1;
			}
		}

}
