package com.woople.streaming.table.udf;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

public class UDFDemo {
	public static void main(String[] args) throws Exception {
		EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
		env.setParallelism(1);

		DataStream<Tuple2<String, String>> data = env.fromElements(
				new Tuple2<>("Mary", "./home#./opt"),
				new Tuple2<>("Bob", "./cart#./tmp#./var"),
				new Tuple2<>("Mary", "./prod?id=1"),
				new Tuple2<>("Liz", "./home"),
				new Tuple2<>("Bob", "./prod?id=3")
		);

		Table clicksTable = tEnv.fromDataStream(data, "user,url");

		tEnv.createTemporaryView("clicks", clicksTable);
		tEnv.registerFunction("split", new Split("#"));

//		clicksTable.joinLateral("split(url) as (path, length)")
//				.select("url, path, length");
//		clicksTable.leftOuterJoinLateral("split(url) as (path, length)")
//				.select("path, length");

		//Table rTable = tEnv.sqlQuery("SELECT url, path, length FROM clicks, LATERAL TABLE(split(url)) as T(path, length)");

		Table rTable = tEnv.sqlQuery("SELECT url, path, length FROM clicks LEFT JOIN LATERAL TABLE(split(url)) as T(path, length) ON TRUE");

		DataStream ds = tEnv.toAppendStream(rTable, TypeInformation.of(new TypeHint<Tuple3<String, String, Long>>(){}));
		ds.print();
		env.execute();
	}
}
