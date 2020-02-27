package com.woople.streaming.yaml;

import org.apache.commons.lang3.RandomUtils;

import java.util.concurrent.*;

public class ThreadPoolTest {
    public static void main(String[] args) {

        try {
            createThreadDemo();
        } catch (RejectedExecutionException e) {
            e.printStackTrace();
            System.exit(-1);
        }

    }

    private static void createThreadDemo() throws RejectedExecutionException {

        // 等待队列
        BlockingQueue<Runnable> workQueue = new ArrayBlockingQueue<>(6);

        // 创建线程池, 核心线程数为5, 最大线程数为10
        ThreadPoolExecutor pool = new ThreadPoolExecutor(5, 10, 600, TimeUnit.SECONDS, workQueue);

        for (int i = 1; i < 15; i++) {

            pool.execute(new Runnable() {
                @Override
                public void run() {
                    System.out.println("===="+Thread.currentThread().getName());
                    try {
                        Thread.sleep(2000L);

                        //System.out.println(Thread.currentThread().getName()+"线程池情况:" + pool.toString());
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });

            System.out.println("创建第" + i + "个线程后, 线程池情况:" + pool.toString());
        }
    }
}
