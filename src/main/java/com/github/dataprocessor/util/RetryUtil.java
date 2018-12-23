package com.github.dataprocessor.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

/**
 * 重试机制
 *
 * @author huangxuyang
 */
public class RetryUtil {
    private static final Logger log = LoggerFactory.getLogger(RetryUtil.class);

    private RetryUtil() {
        throw new UnsupportedOperationException("this util cannot be instantiated");
    }

    /**
     * 重试三次，只要不抛出异常则为成功
     *
     * @param callable 被执行的方法
     * @param <T>      被执行方法的返回值
     * @return 被执行的方法的返回值
     * @throws Exception 最后一次执行被执行方法的时候抛出的异常
     */
    public static <T> T retryCallNullable(Callable<T> callable) throws Exception {
        return retryCall(callable, 3, true);
    }

    /**
     * 重试三次，而且要求被调用方法的返回值不能为空
     *
     * @param callable 被执行的方法
     * @param <T>      被执行方法的返回值
     * @return 最后一次执行被执行方法的返回值
     * @throws Exception 最后一次执行被执行方法的时候抛出的异常
     */
    public static <T> T retryCall(Callable<T> callable) throws Exception {
        return retryCall(callable, 3, false);
    }

    /**
     * 重试执行指定方法
     *
     * @param callable      被执行的方法
     * @param retryTime     重试次数
     * @param retryNullable 是否要求callable.call()返回的值可为空，若false，则当callable.call()返回null会重试
     * @param <T>           最后一次执行被执行方法的返回值
     * @return 被执行的方法的返回值，若重试之后仍然没有成功则返回null
     * @throws Exception 最后一次执行被执行方法的时候抛出的异常
     */
    public static <T> T retryCall(Callable<T> callable, int retryTime, boolean retryNullable) throws Exception {
        int tryTime = 0;
        while (tryTime++ < retryTime) {
            try {
                T t = callable.call();
                if (t != null || retryNullable) {
                    return t;
                } else {
                    log.warn("返回值为空，tryTime: " + tryTime);
                }
            } catch (Exception e) {
                log.error("重试发生异常, tryTime: " + tryTime, e);
                if (tryTime >= retryTime) {
                    throw e;
                }
            }
            try {
                Thread.sleep(retryTime * 500);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }
        return null;
    }

}
