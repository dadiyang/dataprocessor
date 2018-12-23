package com.github.dataprocessor.slice;

import java.util.Set;

/**
 * 分片记录器，用于记录成功和失败的分片，并提供读取功能，子类实现要注意线程安全问题
 *
 * @param <S> 分片的类型
 * @author huangxuyang
 * date 2018/10/26
 */
public interface SliceRecorder<S> {

    /**
     * 保存出错的分片
     *
     * @param slice 分片
     */
    void saveErrorSlice(Slice<S> slice);

    /**
     * 保存完成的分片
     *
     * @param slice 分片
     */
    void saveCompletedSlice(Slice<S> slice);

    /**
     * 保存本批次所有的分片
     *
     * @param slices 分片
     */
    void saveAllSlices(Set<Slice<S>> slices);

    /**
     * 读取最近一次处理失败的分片
     *
     * @return 失败的分片
     */
    Set<Slice<S>> getErrorSlices();

    /**
     * 获取最近保存的所有分片
     *
     * @return 所有分片
     */
    Set<Slice<S>> getAllSlices();

    /**
     * 获取已经完成的分片
     *
     * @return 已经完成的分片
     */
    Set<Slice<S>> getCompletedSlices();

    /**
     * 清除分片历史记录，在启动处理的时候会调用以清理之前处理的分片记录
     */
    void clearRecord();
}
