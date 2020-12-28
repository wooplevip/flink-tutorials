package com.woople.streaming.connector.kafka;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class JDBCDemo {
    public static void main(String[] args) throws Exception {
        Connection connection = DriverManager.getConnection("jdbc:flink://host-10-1-236-128:8083?planner=blink");
        Statement statement = connection.createStatement();

        statement.executeUpdate("CREATE TABLE source (\n" +
                "k INT,\n" +
                "v STRING \n" +
                ") WITH (\n" +
                "  'connector.type' = 'kafka', \n" +
                "  'connector.version' = 'universal',\n" +
                "  'connector.topic' = 'foo',\n" +
                "  'connector.properties.zookeeper.connect' = '10.1.236.130:2181', \n" +
                "  'connector.properties.bootstrap.servers' = '10.1.236.130:6667', \n" +
                "  'connector.properties.group.id' = 'tg1', \n" +
                "  'connector.startup-mode' = 'latest-offset',\n" +
                "  'format.type' = 'csv', \n" +
                "  'format.field-delimiter' = ','\n" +
                ")");

        statement.executeUpdate("CREATE TABLE sink (\n" +
                "k INT,\n" +
                "v STRING \n" +
                ") WITH (\n" +
                "  'connector.type' = 'kafka', \n" +
                "  'connector.version' = 'universal',\n" +
                "  'connector.topic' = 'bar',\n" +
                "  'connector.properties.zookeeper.connect' = '10.1.236.130:2181', \n" +
                "  'connector.properties.bootstrap.servers' = '10.1.236.130:6667', \n" +
                "  'connector.properties.group.id' = 'tg2', \n" +
                "  'connector.startup-mode' = 'latest-offset',\n" +
                "  'format.type' = 'csv', \n" +
                "  'format.field-delimiter' = ',',\n" +
                "'format.line-delimiter' = ''\n" +
                ")");

        //statement.executeUpdate("INSERT INTO sink SELECT * FROM source WHERE k > 5");
        ResultSet rs = statement.executeQuery("INSERT INTO sink SELECT * FROM source WHERE k > 5");
        while (rs.next()) {
            System.out.println(rs.getInt(1) + ", " + rs.getString(2));
        }


        statement.close();
        connection.close();
    }
}
