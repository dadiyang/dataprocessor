package com.github.dataprocessor.threadpool;

import java.util.concurrent.ExecutorService;

/**
 * 线程池工厂接口
 *
 * @author huangxuyang
 * @date 2018/10/28
 */
public interface ThreadPoolFactory {
    /**
     * 根据给定的条件生成线程池
     *
     * @param suggestPoolSize   建议线程池池大小
     * @param threadName 线程池生成的线程名称
     * @return 线程池
     */
    ExecutorService createThreadPool(int suggestPoolSize, String threadName);
}
