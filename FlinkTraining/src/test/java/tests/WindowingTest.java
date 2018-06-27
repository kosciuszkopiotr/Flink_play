package tests;


import io.flinkspector.datastream.DataStreamTestBase;
import io.flinkspector.datastream.input.time.InWindow;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;


public class WindowingTest extends DataStreamTestBase {


    /**
     * Transformation to test.
     * Builds 20 second windows and sums up the integer values.
     *
     * @param stream input {@link DataStream}
     * @return {@link DataStream}
     */
    public static DataStream<Tuple2<Integer, String>> window(DataStream<Tuple2<Integer, String>> stream) {
        return stream.timeWindowAll(Time.of(20, seconds)).sum(0);
    }

    @org.junit.Test
    public void testWindowing() {

        setParallelism(2);

		/*
         * Define the input DataStream:
		 * Get a EventTimeSourceBuilder with, .createTimedTestStreamWith(record).
		 * Add data records to it and retrieve a DataStreamSource
		 * by calling .close().
		 *
		 * Note: The before and after keywords define the time span !between! the previous
		 * record and the current record.
		 */
        DataStream<String> testStream =
                createTimedTestStreamWith("{ \"timestamp/\": \"eloelo\", \"user\": \"A\"}")
                        .emit("{ \"timestamp/\": \"eloelo\", \"user\": \"A\"}")
                        //it's possible to generate unsorted input
                        .emit("{ \"timestamp/\": \"eloelo\", \"user\": \"B\"}")
                        //emit the tuple multiple times, with the time span between:
                        .emit("{ \"timestamp/\": \"eloelo\", \"user\": \"C\"}", InWindow.to(20, seconds), times(2))
                        .close();



    }

}
