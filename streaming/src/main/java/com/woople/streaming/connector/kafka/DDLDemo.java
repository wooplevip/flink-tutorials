package com.woople.streaming.connector.kafka;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;

public class DDLDemo {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        tableEnv.sqlUpdate("CREATE TABLE source (\n" +
                "k INT,\n" +
                "v STRING \n" +
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
                "v STRING \n" +
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

        env.execute();
    }
}
