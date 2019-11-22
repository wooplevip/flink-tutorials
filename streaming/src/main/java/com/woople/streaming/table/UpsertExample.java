package com.woople.streaming.table;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sinks.UpsertStreamTableSink;

public class UpsertExample {
	public static void main(String[] args) throws Exception {
		EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
		env.setParallelism(1);

		DataStream<Tuple2<String, String>> data = env.fromElements(
				new Tuple2<>("Mary", "./home"),
				new Tuple2<>("Bob", "./cart"),
				new Tuple2<>("Mary", "./prod?id=1"),
				new Tuple2<>("Liz", "./home"),
				new Tuple2<>("Liz", "./prod?id=3"),
				new Tuple2<>("Mary", "./prod?id=7")
		);

		Table clicksTable = tEnv.fromDataStream(data, "user,url");

		tEnv.registerTable("clicks", clicksTable);
		Table rTable = tEnv.sqlQuery("SELECT user,COUNT(url) FROM clicks GROUP BY user");
		tEnv.registerTableSink("MemoryUpsertSink", new MemoryUpsertSink(rTable.getSchema()));
		rTable.insertInto("MemoryUpsertSink");

		env.execute();
	}

	private static class MemoryUpsertSink implements UpsertStreamTableSink<Tuple2<String, Long>> {
		private TableSchema schema;
		private String[] keyFields;
		private boolean isAppendOnly;

		private String[] fieldNames;
		private TypeInformation<?>[] fieldTypes;

		public MemoryUpsertSink() {

		}

		public MemoryUpsertSink(TableSchema schema) {
			this.schema = schema;
		}

		@Override
		public void setKeyFields(String[] keys) {
			this.keyFields = keys;
		}

		@Override
		public void setIsAppendOnly(Boolean isAppendOnly) {
			this.isAppendOnly = isAppendOnly;
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
			MemoryUpsertSink memoryUpsertSink = new MemoryUpsertSink();
			memoryUpsertSink.setFieldNames(fieldNames);
			memoryUpsertSink.setFieldTypes(fieldTypes);
			memoryUpsertSink.setKeyFields(keyFields);
			memoryUpsertSink.setIsAppendOnly(isAppendOnly);

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

	private static class DataSink extends RichSinkFunction<Tuple2<Boolean, Tuple2<String, Long>>> {
		public DataSink() {
		}

		@Override
		public void invoke(Tuple2<Boolean, Tuple2<String, Long>> value, Context context) throws Exception {
			System.out.println("send message:" + value);
		}
	}
}
