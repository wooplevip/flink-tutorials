package com.woople.streaming.window;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;

public class TumblingWindowsEventTimeDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Tuple3<String, String, Long>> orangeStream = env.addSource(new DataSource())
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessGenerator());

        orangeStream.keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.seconds(30)))
                .allowedLateness(Time.seconds(10))
                .apply(new WindowFunction<Tuple3<String, String, Long>, Object, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple3<String, String, Long>> input, Collector<Object> out) throws Exception {
                        System.out.println(window.toString());
                        out.collect(input);
                    }
                }).name("EventTimeWindow").print("out");

        env.execute("EventTime Demo");
    }

    private static class DataSource extends RichParallelSourceFunction<Tuple3<String, String, Long>> {
        private volatile boolean running = true;

        @Override
        public void run(SourceContext<Tuple3<String, String, Long>> ctx) throws Exception {

            Tuple3<String, String, Long>[] datas = new Tuple3[]{
                    new Tuple3("foo", "11:11:21", 1573441881000L),
                    new Tuple3("foo", "11:11:57", 1573441917000L),
                    new Tuple3("foo", "11:12:14", 1573441934000L),
                    new Tuple3("foo", "11:11:50", 1573441910000L),
                    new Tuple3("foo", "11:11:52", 1573441912000L),

                    new Tuple3("foo", "11:12:41", 1573441961000L),

                    new Tuple3("foo", "11:11:33", 1573441893000L),

                    new Tuple3("foo", "11:12:23", 1573441943000L),
                    new Tuple3("foo", "11:12:30", 1573441950000L),
                    new Tuple3("foo", "11:13:04", 1573441984000L)
            };


            final int numElements = datas.length;
            int i = 0;

            while (running && i < numElements) {
                Thread.sleep(2000);
                ctx.collect(datas[i]);
                System.out.println("sand data:" + datas[i]);
                i++;
            }
            Thread.sleep(15000);
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    private static class BoundedOutOfOrdernessGenerator implements AssignerWithPeriodicWatermarks<Tuple3<String, String, Long>> {
        private final long maxOutOfOrderness = 10000;
        private long currentMaxTimestamp;

        @Override
        public long extractTimestamp(Tuple3<String, String, Long> row, long previousElementTimestamp) {
            long timestamp = row.f2;
            currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
            System.out.println(Thread.currentThread().getId() + "-" + row + ",time="+stampToDate(row.f2.toString()) + ",watermark=" + stampToDate(String.valueOf(currentMaxTimestamp - maxOutOfOrderness)));
            return timestamp;
        }

        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
        }

        private static String stampToDate(String s) {
            String res;
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            long lt = new Long(s);
            Date date = new Date(lt);
            res = simpleDateFormat.format(date);
            return res;
        }
    }
}
