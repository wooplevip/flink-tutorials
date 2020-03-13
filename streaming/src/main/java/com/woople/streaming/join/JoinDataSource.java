package com.woople.streaming.join;

import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class JoinDataSource extends RichParallelSourceFunction<Tuple3<String, String, Long>> {
    private volatile boolean running = true;
    private volatile String name;
    private volatile Tuple3[] data;

    public JoinDataSource(String name) {
        this.name = name;
    }

    public JoinDataSource(String name, Tuple3[] data) {
        this.name = name;
        this.data = data;
    }

    @Override
    public void run(SourceContext<Tuple3<String, String, Long>> ctx) throws Exception {
        final long numElements = data.length;
        int i = 0;
        while (running && i < numElements) {
            Thread.sleep(RandomUtils.nextLong(1, 5) * 1000L);
            ctx.collect(data[i]);
            //System.out.println(this.name + "-sand data:" + data[i]);
            i++;
        }

        ctx.collect(new Tuple3<>("source1","source1",0L));
        Thread.sleep(6000L);
    }

    @Override
    public void cancel() {
        running = false;
    }
}
