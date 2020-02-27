package com.woople.streaming.yaml;

import org.apache.commons.configuration2.YAMLConfiguration;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;
import java.io.FileInputStream;

public class LoadYaml {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        Configuration config = new Configuration();
        config.setString("a", "b");
        env.fromElements("1")
                .map(new RichMapFunction<String, String>() {
                    private YAMLConfiguration config;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        config = new YAMLConfiguration();
                        config.read(new FileInputStream(new File("/Users/peng/SandBox/Dev/MyBranch/flink-tutorials/streaming/src/main/resources/test.yaml")));
                    }

                    @Override
                    public String map(String value) throws Exception {
                        return config.getString("com.woople.test");
                    }
                }).print();

        env.execute("demo");
    }
}
