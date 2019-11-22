package com.woople.streaming.table;

import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sinks.UpsertStreamTableSink;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;


public class StreamSQLExample {

	public static void main(String[] args) throws Exception {

		// set up execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
		env.setParallelism(1);

		DataStream<Click> clicks = env.addSource(new DataSource()).keyBy("user");


		Table clicksTable = tEnv.fromDataStream(clicks, "user,url");
		//Table rTable = clicksTable.select("user, url").where("user='Mary'");
//		Table rTable = clicksTable.window(Tumble.over("10.seconds").on("UserActionTime").as("userActionWindow"))
//				.groupBy("user, userActionWindow").select("user, url.count as cnt");
		//Table rTable = clicksTable.groupBy("user").select("user, url.count as cnt");

		tEnv.registerTable("a", clicksTable);


		//tEnv.toRetractStream(rTable, TypeInformation.of(new TypeHint<Tuple2<String, Long>>(){})).print();

//		DataStream ds = tEnv.toRetractStream(rTable, TypeInformation.of(new TypeHint<Tuple2<String, Long>>(){}));
//		ds.print();
		final TableSchema schema = TableSchema.builder()
				.field("user", DataTypes.STRING())
				.field("cnt", DataTypes.STRING())
				.build();


		tEnv.registerTableSink("bb", new MemoryUpsertSink(schema));



		tEnv.sqlUpdate("INSERT INTO bb SELECT user, url as cnt from a ");



		//rTable.insertInto("bb");

		env.execute();
	}
	public static class MemoryRetractStream implements RetractStreamTableSink<Tuple2<String, Long>>{
		private TableSchema schema;

		public MemoryRetractStream(TableSchema schema) {
			this.schema = schema;
		}

		@Override
		public TypeInformation<Tuple2<String, Long>> getRecordType() {
			return TypeInformation.of(new TypeHint<Tuple2<String, Long>>(){});
		}

		@Override
		public void emitDataStream(DataStream<Tuple2<Boolean, Tuple2<String, Long>>> dataStream) {
			//dataStream.addSink(new DataSink());
		}

		@Override
		public TableSink<Tuple2<Boolean, Tuple2<String, Long>>> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
			return null;
		}

		@Override
		public String[] getFieldNames() {
			return schema.getFieldNames();
		}

		@Override
		public TypeInformation<?>[] getFieldTypes() {
			return schema.getFieldTypes();
		}
	}

	public static class MemoryUpsertSink implements UpsertStreamTableSink<Tuple2<String, String>> {
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
			this.keyFields = new String[]{this.schema.getFieldNames()[0]};
		}

		@Override
		public void setIsAppendOnly(Boolean isAppendOnly) {
			this.isAppendOnly = isAppendOnly;
			System.out.println("==========####isAppendOnly="+isAppendOnly);
		}

		@Override
		public TypeInformation<Tuple2<String, String>> getRecordType() {
			return TypeInformation.of(new TypeHint<Tuple2<String, String>>(){});
		}

		@Override
		public void emitDataStream(DataStream<Tuple2<Boolean, Tuple2<String, String>>> dataStream) {
			dataStream.addSink(new DataSink());
		}

		@Override
		public TableSink<Tuple2<Boolean, Tuple2<String, String>>> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
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

	private static class DataSink extends RichSinkFunction<Tuple2<Boolean, Tuple2<String, String>>>{
		private volatile ConcurrentHashMap<String, Tuple2<String, String>> data = new ConcurrentHashMap<>();

		public DataSink() {
		}

		@Override
		public void invoke(Tuple2<Boolean, Tuple2<String, String>> value, Context context) throws Exception {

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
