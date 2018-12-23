package com.github.dataprocessor.threadpool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * 如果 task 被拒绝，则通过阻塞方式加入到队列中，以此减慢生产者的速度
 *
 * @author huangxuyang-sz
 * date 2018/07/19
 */
public class BlockRejectedExecutionHandler implements RejectedExecutionHandler {
    private static final Logger logger = LoggerFactory.getLogger(BlockRejectedExecutionHandler.class);

    @Override
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
        logger.warn("任务超载: 尝试阻塞式加入任务队列:" + r);
        if (!executor.isShutdown()) {
            try {
                logger.warn("任务超载: 尝试阻塞式加入任务队列");
                executor.getQueue().put(r);
            } catch (InterruptedException e) {
                logger.error("将被拒绝的 task 阻塞式加入任务队列时发生异常", e);
                //保持线程的中端状态
                Thread.currentThread().interrupt();
            }
        } else {
            logger.warn("线程池已关闭，不再执行此任务:" + r);
        }
    }
}
