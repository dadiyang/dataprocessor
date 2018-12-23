package com.github.dataprocessor;

/**
 * 数据处理器，此项目的核心接口
 *
 * @author huangxuyang
 * @date 2018/10/27
 */
public interface DataProcessor {
    /**
     * 处理数据
     *
     * @return 任务是否全部成功
     */
    boolean process();

    /**
     * 处理失败的分片
     *
     * @return 任务是否全部成功
     */
    boolean processErrorSlices();

    /**
     * 恢复上次未完成的任务（断点续传）
     *
     * @return 任务是否全部成功
     */
    boolean resumeProgress();
}
