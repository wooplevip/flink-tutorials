package com.woople.streaming.table;

import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sinks.UpsertStreamTableSink;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;


public class StreamSQLDemo {

	public static void main(String[] args) throws Exception {
		final EnvironmentSettings.Builder builder = EnvironmentSettings.newInstance();
		builder.inStreamingMode();


			builder.useBlinkPlanner();


		final EnvironmentSettings settings = builder.build();

		// set up execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
		env.setParallelism(1);



		DataStream<Tuple2<Long, String>> myInts = env.fromElements(
//				new Tuple3<>(1, 1L, "A"),
				new Tuple2<>(2L, "B"),
				new Tuple2<>(2L, "B"),
				new Tuple2<>(3L, "C"),
				new Tuple2<>(0L, "C")
				);


		Table clicksTable = tEnv.fromDataStream(myInts, "b,c");
		Table rTable = clicksTable.groupBy("c").select("c, b.sum as maxV");


		//tEnv.registerTable("a", clicksTable);

		final TableSchema schema = TableSchema.builder()
				.field("c", DataTypes.STRING())
				.field("maxV", DataTypes.BIGINT())
				.build();


		tEnv.registerTableSink("testSink", new MemoryUpsertSink(rTable.getSchema()));

		//tEnv.sqlUpdate("INSERT INTO bb SELECT user, cnt from a ");

		rTable.insertInto(new StreamQueryConfig(2,300005), "testSink");

		env.execute();
	}

	public static class MemoryUpsertSink implements UpsertStreamTableSink<Tuple2<String, Long>> {
		private TableSchema schema;
		private String[] keyFields;
		private boolean isAppendOnly;

		private String[] fieldNames;
		private TypeInformation<?>[] fieldTypes;

		public MemoryUpsertSink(String[] fieldNames) {
			this.fieldNames = fieldNames;
		}

		public MemoryUpsertSink(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
			this.fieldNames = fieldNames;
			this.fieldTypes = fieldTypes;
		}

		public MemoryUpsertSink(TableSchema schema) {
			this.schema = schema;
		}

		@Override
		public void setKeyFields(String[] keys) {
			for (String key : keys){
				System.out.println(key + "======");
			}
			this.keyFields = new String[]{this.schema.getFieldNames()[0]};
		}

		@Override
		public void setIsAppendOnly(Boolean isAppendOnly) {
			this.isAppendOnly = isAppendOnly;
			System.out.println("==========####isAppendOnly="+isAppendOnly);
		}

		@Override
		public TypeInformation<Tuple2<String, Long>> getRecordType() {
			return TypeInformation.of(new TypeHint<Tuple2<String, Long>>(){});
		}

		@Override
		public void emitDataStream(DataStream<Tuple2<Boolean, Tuple2<String, Long>>> dataStream) {
			consumeDataStream(dataStream);

		}

		@Override
		public DataStreamSink<?> consumeDataStream(DataStream<Tuple2<Boolean, Tuple2<String, Long>>> dataStream) {
			return dataStream.addSink(new DataSink()).setParallelism(1);
		}

		@Override
		public TableSink<Tuple2<Boolean, Tuple2<String, Long>>> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
			MemoryUpsertSink memoryUpsertSink = new MemoryUpsertSink(fieldNames);
			memoryUpsertSink.setFieldNames(fieldNames);
			memoryUpsertSink.setFieldTypes(fieldTypes);

			return memoryUpsertSink;
		}

		@Override
		public String[] getFieldNames() {
			return schema.getFieldNames();
		}

		public void setFieldNames(String[] fieldNames) {
			this.fieldNames = fieldNames;
		}

		@Override
		public TypeInformation<?>[] getFieldTypes() {
			return schema.getFieldTypes();
		}

		public void setFieldTypes(TypeInformation<?>[] fieldTypes) {
			this.fieldTypes = fieldTypes;
		}
	}


	/**
	 * Simple POJO.
	 */
	public static class Click {
		public String user;
		public String url;

		public Click(String user, String url) {
			this.user = user;
			this.url = url;
		}

		public Click() {
		}

		@Override
		public String toString() {
			return "Click{" +
					"user='" + user + '\'' +
					", url='" + url + '\'' +
					'}';
		}
	}

	private static class DataSink extends RichSinkFunction<Tuple2<Boolean, Tuple2<String, Long>>>{
		private volatile ConcurrentHashMap<String, Tuple2<String, String>> data = new ConcurrentHashMap<>();

		public DataSink() {
		}

		@Override
		public void invoke(Tuple2<Boolean, Tuple2<String, Long>> value, Context context) throws Exception {

			System.out.println("===" + value);

		}
	}

	private static class DataSource extends RichParallelSourceFunction<Click> {
		private volatile boolean running = true;

		@Override
		public void run(SourceContext<Click> ctx) throws Exception {
			int bound = 50;
			String[] users = new String[]{"Mary", "Bob", "Liz"};

			final long numElements = RandomUtils.nextLong(10, 20);
			int i = 0;
			while (running && i < numElements) {
				Thread.sleep(RandomUtils.nextLong(1, 5) * 1000L);
				Click data = new Click(users[RandomUtils.nextInt(0, 3)], "./prod?id="+RandomUtils.nextInt(1, 10));
				ctx.collect(data);
				System.out.println(Thread.currentThread().getId() + "-sand data:" + data);
				i++;
			}
		}

		@Override
		public void cancel() {
			running = false;
		}

		private long getRandomTime(int min, int max){
			return 1573441860000L + 1000* RandomUtils.nextInt(min, max);
		}
	}
}
