package com.woople.streaming.state;

import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class StateDataSource extends RichParallelSourceFunction<Tuple3<String, String, Long>> {
    private volatile boolean running = true;

    @Override
    public void run(SourceContext<Tuple3<String, String, Long>> ctx) throws Exception {
        String[] areas = new String[]{"A", "B"};
        String[] phoneNumbers = new String[]{"19911111111", "19911111112"};

        long numElements = RandomUtils.nextLong(10, 30);

        int i = 0;
        while (running && i < numElements) {
            Thread.sleep(RandomUtils.nextLong(1, 3) * 1000L);//areas[RandomUtils.nextInt(0, 3)]
            Tuple3 data = new Tuple3<>("19911111111", areas[RandomUtils.nextInt(0, 2)], System.currentTimeMillis());//phoneNumbers[RandomUtils.nextInt(0, 2)]
            ctx.collect(data);
            System.out.println("Sand data:" + data);
            i++;
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
