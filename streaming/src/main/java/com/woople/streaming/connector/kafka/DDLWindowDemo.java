package com.woople.streaming.connector.kafka;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;
/**
 * 输入3条数据
 * 1,2,2020-04-01 18:00:30
 * 1,3,2020-04-01 18:00:31
 * 1,9,2020-04-01 18:10:31
 *
 * 输出结果
 * 1,"2020-04-01 18:00:30",5
 *
 * **/

public class DDLWindowDemo {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        tableEnv.sqlUpdate("CREATE TABLE source (\n" +
                "k INT,\n" +
                "v INT,\n" +
                "row_time TIMESTAMP(3),\n" +
                "WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND \n" +
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
                "wStart TIMESTAMP(3),\n" +
                "s INT \n" +
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


        tableEnv.sqlUpdate("INSERT INTO sink SELECT k,  TUMBLE_START(row_time, INTERVAL '5' SECOND) as wStart,  SUM(v) as s FROM source GROUP BY TUMBLE(row_time, INTERVAL '5' SECOND), k");

        env.execute();
    }
}
