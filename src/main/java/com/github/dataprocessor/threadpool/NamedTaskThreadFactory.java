package com.github.dataprocessor.threadpool;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 线程创建工厂，生产带名字的线程
 *
 * @author huangxuyang
 * @date 2018/7/26
 */
public class NamedTaskThreadFactory implements ThreadFactory {
    private final String threadName;
    private AtomicInteger id;

    public NamedTaskThreadFactory(String threadName) {
        this.threadName = threadName;
        id = new AtomicInteger(0);
    }

    @Override
    public Thread newThread(Runnable r) {
        return new Thread(r, threadName + "-" + id.getAndIncrement());
    }
}
