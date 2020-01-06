package com.woople.streaming.sql;

import com.woople.streaming.utils.MemoryRetractStreamTableSink;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.TableSink;

public class StreamingGroupBySqlDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);

        DataStream<Tuple3<Long, String, Integer>> ds = env.addSource(new DataSource());
        tableEnv.registerDataStream("Orders", ds, "user, product, amount");

        Table result = tableEnv.sqlQuery("SELECT user, product, SUM(amount) as amounts FROM Orders GROUP BY user, product");

        String[] fieldNames = {"user", "product", "amounts"};
        TypeInformation[] fieldTypes = {Types.LONG, Types.STRING, Types.INT};

        TableSink sink = new MemoryRetractStreamTableSink(fieldNames, fieldTypes);
        tableEnv.registerTableSink("output", sink);
        result.insertInto("output");

        env.execute();
    }

    private static class DataSource extends RichParallelSourceFunction<Tuple3<Long, String, Integer>> {
        private volatile boolean running = true;

        @Override
        public void run(SourceContext<Tuple3<Long, String, Integer>> ctx) throws Exception {
            String[] products = new String[]{"iPhoneX", "iPhone11", "iPhone11 Pro Max"};

            final long numElements = 10;
            int i = 0;
            while (running && i < numElements) {
                Thread.sleep(RandomUtils.nextLong(1, 5) * 1000L);
                Tuple3 data = new Tuple3<Long, String, Integer>(RandomUtils.nextLong(1, 2), products[RandomUtils.nextInt(0, 3)],RandomUtils.nextInt(1, 5)*1000);
                ctx.collect(data);
                System.out.println("sand data:" + data);
                i++;
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
