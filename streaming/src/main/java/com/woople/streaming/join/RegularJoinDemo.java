package com.woople.streaming.join;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

public class RegularJoinDemo {
	public static void main(String[] args) throws Exception {
		EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
		env.setParallelism(1);

		DataStream<Tuple2<String, String>> orangeStream = env.fromElements(
				new Tuple2<>("Mary", "./home"),
				new Tuple2<>("Bob", "./cart"),
				new Tuple2<>("Mary", "./prod?id=1"),
				new Tuple2<>("Liz", "./home"),
				new Tuple2<>("Bob", "./prod?id=3"),
				new Tuple2<>("Tom", "./opt")
		);

		DataStream<Tuple2<String, String>> greenStream = env.fromElements(
				new Tuple2<>("Mary", "20"),
				new Tuple2<>("Bob", "21"),
				new Tuple2<>("Mary", "20"),
				new Tuple2<>("Liz", "18"),
				new Tuple2<>("Bob", "21")
		);

		Table clicksTable = tEnv.fromDataStream(orangeStream, "name,url");
		Table userInfo = tEnv.fromDataStream(greenStream, "name,age");

		tEnv.registerTable("clicks", clicksTable);
		tEnv.registerTable("users", userInfo);

		Table rTable = tEnv.sqlQuery("SELECT clicks.name, clicks.url, users.age FROM clicks INNER JOIN users ON clicks.name = users.name ");

		DataStream ds = tEnv.toAppendStream(rTable, TypeInformation.of(new TypeHint<Tuple3<String, String, String>>(){}));
		ds.print();
		env.execute();
	}
}
