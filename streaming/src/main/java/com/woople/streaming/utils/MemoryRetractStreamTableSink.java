package com.woople.streaming.utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

public class MemoryRetractStreamTableSink implements RetractStreamTableSink<Row> {
    private String[] fieldNames;
    private TypeInformation<?>[] fieldTypes;

    public MemoryRetractStreamTableSink(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
    }

    @Override
    public TableSink<Tuple2<Boolean, Row>> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        return this;
    }

    @Override
    public void emitDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
        consumeDataStream(dataStream);
    }

    @Override
    public DataStreamSink<?> consumeDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
        return dataStream.addSink(new DataSink()).setParallelism(dataStream.getParallelism());
    }

    @Override
    public TypeInformation<Row> getRecordType() {
        return new RowTypeInfo(getFieldTypes(), getFieldNames());
    }

    @Override
    public String[] getFieldNames() {
        return fieldNames;
    }

    @Override
    public TypeInformation<?>[] getFieldTypes() {
        return fieldTypes;
    }

    private class DataSink extends RichSinkFunction<Tuple2<Boolean, Row>> {
        public DataSink() {
        }

        @Override
        public void invoke(Tuple2<Boolean, Row> value, Context context) throws Exception {
            System.out.println("Result:" + value);
        }
    }
}
