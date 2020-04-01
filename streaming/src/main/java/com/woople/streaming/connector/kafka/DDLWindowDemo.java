package com.woople.streaming.connector.kafka;

import com.woople.streaming.utils.MemoryAppendStreamTableSink;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.TableSink;

public class DDLDemo {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        tableEnv.sqlUpdate("CREATE TABLE source (\n" +
                "k INT,\n" +
                "v STRING,\n" +
                "order_time TIMESTAMP(3),\n" +
                "WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND \n" +
                ") WITH (\n" +
                "  'connector.type' = 'kafka', \n" +
                "  'connector.version' = 'universal',\n" +
                "  'connector.topic' = 'foo',\n" +
                "  'connector.properties.zookeeper.connect' = '10.1.236.67:2181/kafka-2.3.0-2', \n" +
                "  'connector.properties.bootstrap.servers' = '10.1.236.67:9092', \n" +
                "  'connector.properties.group.id' = 'tg1', \n" +
                "  'connector.startup-mode' = 'latest-offset',\n" +
                "  'format.type' = 'csv', \n" +
                "  'format.field-delimiter' = ','\n" +
                ")");

        tableEnv.sqlUpdate("CREATE TABLE sink (\n" +
                "k INT,\n" +
                "v STRING,\n" +
                "order_time TIMESTAMP(3)\n" +
                ") WITH (\n" +
                "  'connector.type' = 'kafka', \n" +
                "  'connector.version' = 'universal',\n" +
                "  'connector.topic' = 'bar',\n" +
                "  'connector.properties.zookeeper.connect' = '10.1.236.67:2181/kafka-2.3.0-2', \n" +
                "  'connector.properties.bootstrap.servers' = '10.1.236.67:9092', \n" +
                "  'connector.properties.group.id' = 'tg2', \n" +
                "  'connector.startup-mode' = 'latest-offset',\n" +
                "  'format.type' = 'csv', \n" +
                "  'format.field-delimiter' = ',',\n" +
                "'format.line-delimiter' = ''\n" +
                ")");



        tableEnv.sqlUpdate("INSERT INTO sink SELECT * FROM source WHERE k > 5");



//        DataStream ds = tableEnv.toAppendStream(result, TypeInformation.of(new TypeHint<Tuple2<String, String>>(){}));
//        ds.print();

        env.execute();
    }
}
