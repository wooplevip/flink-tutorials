package com.woople.streaming.operators;

import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

public class IntervalJoinDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1);
        //Time-bounded stream joins are only supported in event time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Tuple3<String, Integer, Long>> orangeStream = env.addSource(new DataSource("orangeStream")).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessGenerator());
        DataStream<Tuple3<String, Integer, Long>> greenStream = env.addSource(new DataSource("greenStream")).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessGenerator());

        orangeStream
                .keyBy(0)
                .intervalJoin(greenStream.keyBy(0))
                .between(Time.seconds(-5), Time.seconds(5))
                .process(new ProcessJoinFunction<Tuple3<String, Integer, Long>, Tuple3<String, Integer, Long>, Object>() {
                    @Override
                    public void processElement(Tuple3<String, Integer, Long> left, Tuple3<String, Integer, Long> right, Context ctx, Collector<Object> out) throws Exception {
                        out.collect(new Tuple5<>(left.f0, left.f1, left.f2, right.f1, right.f2));
                    }
                }).name("intervalJoin").print("xxxxxx");

        env.execute("Interval Join Demo");
    }

    private static class DataSource extends RichParallelSourceFunction<Tuple3<String, Integer, Long>> {

        private volatile boolean running = true;
        private volatile String name;

        public DataSource(String name) {
            this.name = name;
        }

        @Override
        public void run(SourceContext<Tuple3<String, Integer, Long>> ctx) throws Exception {
            Random random = new Random();
            int bound = 100;
            Tuple3[] data = new Tuple3[]{
                    new Tuple3<>("foo", random.nextInt(bound), getRandomInt(50, 70)), new Tuple3<>("foo", random.nextInt(bound),  getRandomInt(40, 60))};
            final long numElements = data.length;
            int i = 0;
            while (running && i < numElements) {
                Thread.sleep(RandomUtils.nextLong(1, 5) * 1000L);
                ctx.collect(data[i]);
                System.out.println(Thread.currentThread().getId() + "-" + this.name + "-sand data:" + data[i]);
                i++;
            }
        }

        @Override
        public void cancel() {
            running = false;
        }

        private long getRandomInt(int min, int max){
            return 1573441870000L + 1000*(new Random().nextInt(max-min+1)+min);
        }
    }

    private static class BoundedOutOfOrdernessGenerator implements AssignerWithPeriodicWatermarks<Tuple3<String, Integer, Long>> {

        private final long maxOutOfOrderness = 10000;

        private long currentMaxTimestamp;

        @Override
        public long extractTimestamp(Tuple3<String, Integer, Long> row, long previousElementTimestamp) {
            System.out.println(Thread.currentThread().getId() + "-" + row + ",time="+stampToDate(row.f2.toString()));
            long timestamp = row.f2;
            currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
            System.out.println(Thread.currentThread().getId() + "-watermark:" + stampToDate(String.valueOf(currentMaxTimestamp - maxOutOfOrderness)));
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
