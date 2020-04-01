package com.woople.streaming.connector.kafka;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * source1的数据
 * Mary,./home,2020-03-13 1:30:30
 * Liz,./opt,2020-03-13 1:30:38
 * Bob,./cart,2020-03-13 1:30:40
 *
 * source2的数据
 * Mary,A,2020-03-13 1:30:28
 * Liz,C,2020-03-13 1:30:42
 * Bob,B,2020-03-13 1:30:44
 *
 * 输出结果
 * Mary,./home,A
 * Liz,./opt,C
 * Bob,./cart,B
 * **/

public class DDLJoinDemo {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        tableEnv.sqlUpdate("CREATE TABLE source1 (\n" +
                "k1 STRING,\n" +
                "v1 STRING,\n" +
                "row_time1 TIMESTAMP(3),\n" +
                "WATERMARK FOR row_time1 AS row_time1 - INTERVAL '10' SECOND \n" +
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
        tableEnv.sqlUpdate("CREATE TABLE source2 (\n" +
                "k2 STRING,\n" +
                "v2 STRING,\n" +
                "row_time2 TIMESTAMP(3),\n" +
                "WATERMARK FOR row_time2 AS row_time2 - INTERVAL '10' SECOND \n" +
                ") WITH (\n" +
                "  'connector.type' = 'kafka', \n" +
                "  'connector.version' = 'universal',\n" +
                "  'connector.topic' = 'bar',\n" +
                "  'connector.properties.zookeeper.connect' = '10.1.236.67:2181/kafka-2.3.0-2', \n" +
                "  'connector.properties.bootstrap.servers' = '10.1.236.67:9092', \n" +
                "  'connector.properties.group.id' = 'tg1', \n" +
                "  'connector.startup-mode' = 'latest-offset',\n" +
                "  'format.type' = 'csv', \n" +
                "  'format.field-delimiter' = ','\n" +
                ")");

        tableEnv.sqlUpdate("CREATE TABLE sink (\n" +
                "k1 STRING,\n" +
                "v1 STRING,\n" +
                "v2 STRING \n" +
                ") WITH (\n" +
                "  'connector.type' = 'kafka', \n" +
                "  'connector.version' = 'universal',\n" +
                "  'connector.topic' = 'baz',\n" +
                "  'connector.properties.zookeeper.connect' = '10.1.236.67:2181/kafka-2.3.0-2', \n" +
                "  'connector.properties.bootstrap.servers' = '10.1.236.67:9092', \n" +
                "  'connector.properties.group.id' = 'tg2', \n" +
                "  'connector.startup-mode' = 'latest-offset',\n" +
                "  'format.type' = 'csv', \n" +
                "  'format.field-delimiter' = ',',\n" +
                "'format.line-delimiter' = ''\n" +
                ")");
        

        tableEnv.sqlUpdate("INSERT INTO sink SELECT s1.k1, s1.v1, s2.v2 FROM source1 s1  join source2 s2 on s1.k1 = s2.k2 where s2.row_time2 BETWEEN s1.row_time1 - INTERVAL '5' SECOND AND s1.row_time1 + INTERVAL '5' SECOND ");

        env.execute();
    }
}
