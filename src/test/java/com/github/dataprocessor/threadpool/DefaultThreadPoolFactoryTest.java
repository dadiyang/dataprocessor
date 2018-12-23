package com.github.dataprocessor.threadpool;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * 测试默认线程池工厂，包括线程名和线程池大小
 *
 * @author huangxuyang
 * @date 2018/10/28
 */
public class DefaultThreadPoolFactoryTest {
    private DefaultThreadPoolFactory factory = new DefaultThreadPoolFactory();

    @Test
    public void createThreadPool() {
        int size = 8;
        String name = "testThreadPool";
        ExecutorService executorService = factory.createThreadPool(size, name);
        Callable<?> callable = (Callable<Object>) () -> {
            // 测试检查线程名称是否正确
            String currentThreadName = Thread.currentThread().getName();
            assertTrue(currentThreadName.startsWith(name));
            return null;
        };
        List<Future> futures = new ArrayList<>(size * 10);
        for (int i = 0; i < size * 10; i++) {
            futures.add(executorService.submit(callable));
        }
        for (Future future : futures) {
            try {
                future.get();
            } catch (Exception e) {
                // 若有任何一个报异常则不通过
                fail("线程名称都应该以 " + name + " 开头");
            }
        }
        assertTrue(executorService instanceof ThreadPoolExecutor);
        ThreadPoolExecutor executor = (ThreadPoolExecutor) executorService;
        // 测试线程池大小是否正确
        assertEquals("核心线程数", size, executor.getCorePoolSize());
        assertEquals("最大线程数", size, executor.getMaximumPoolSize());
    }
}