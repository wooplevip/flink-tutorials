package com.woople.streaming.join;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

public class TimeWindowedJoinDemo {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useOldPlanner().build();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1);
        //Time-bounded stream joins are only supported in event time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        Tuple3[] clickData = new Tuple3[]{
                new Tuple3<>("Mary", "./home", 1584034230000L),//2020-03-13 1:30:30
                new Tuple3<>("Liz", "./opt", 1584034238000L),//2020-03-13 1:30:38
                new Tuple3<>("Bob", "./cart", 1584034240000L)//2020-03-13 1:30:40
        };

        Tuple3[] locationData = new Tuple3[]{
                new Tuple3<>("Mary", "A", 1584034228000L),//2020-03-13 1:30:28
                new Tuple3<>("Liz", "C", 1584034242000L),//2020-03-13 1:30:42
                new Tuple3<>("Bob", "B", 1584034244000L)//2020-03-13 1:30:44
        };

        DataStream<Tuple3<String, String, Long>> orangeStream = env.addSource(new JoinDataSource("orangeStream", clickData)).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, String, Long>>(Time.seconds(10)) {
            @Override
            public long extractTimestamp(Tuple3<String, String, Long> element) {
                return element.f2;
            }
        });
        DataStream<Tuple3<String, String, Long>> greenStream = env.addSource(new JoinDataSource("greenStream", locationData)).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, String, Long>>(Time.seconds(10)) {
            @Override
            public long extractTimestamp(Tuple3<String, String, Long> element) {
                return element.f2;
            }
        });
        Table clicksTable = tEnv.fromDataStream(orangeStream, "name,url,rowtime.rowtime");
        Table userLocation = tEnv.fromDataStream(greenStream, "name,location,rowtime.rowtime");

        tEnv.createTemporaryView("clicks", clicksTable);
        tEnv.createTemporaryView("users", userLocation);

        Table rTable = tEnv.sqlQuery("SELECT c.name, c.url, u.location FROM clicks c, users u where c.name = u.name " +
                " and c.rowtime BETWEEN u.rowtime - INTERVAL '5' SECOND AND u.rowtime + INTERVAL '5' SECOND ");

        DataStream ds = tEnv.toAppendStream(rTable, TypeInformation.of(new TypeHint<Tuple3<String, String, String>>(){}));
        ds.print("TimeWindowed Join Demo : ");
        env.execute("TimeWindowed Join Demo");
    }
}
