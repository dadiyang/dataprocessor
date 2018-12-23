package com.github.dataprocessor.threadpool;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 线程池
 * 拒绝策略使用阻塞式，即当队列满时再添加任务将会被阻塞
 * 线程工厂为每个线程命名
 *
 * @author huangxuyang
 * @date 2018/10/28
 */
public class DefaultThreadPoolFactory implements ThreadPoolFactory {
    /**
     * 线程池维护线程所允许的空闲时间
     */
    private final static int KEEP_ALIVE_TIME = 60;
    /**
     * 线程池所使用的缓冲队列大小的默认值
     */
    private final static int DEFAULT_WORK_QUEUE_SIZE = 1024;
    /**
     * 线程池所使用的缓冲队列大小
     */
    private int queueSize;

    public DefaultThreadPoolFactory(int queueSize) {
        this.queueSize = queueSize;
    }

    public DefaultThreadPoolFactory() {
        queueSize = DEFAULT_WORK_QUEUE_SIZE;
    }

    /**
     * 生成固定大小的线程池，并且为线程命名
     *
     * @param suggestPoolSize 建议线程池池大小
     * @param threadName      线程池生成的线程名称
     * @return 线程池
     */
    @Override
    public ExecutorService createThreadPool(int suggestPoolSize, String threadName) {
        return new ThreadPoolExecutor(suggestPoolSize,
                suggestPoolSize,
                KEEP_ALIVE_TIME,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(queueSize),
                new NamedTaskThreadFactory(threadName),
                new BlockRejectedExecutionHandler());
    }
}

