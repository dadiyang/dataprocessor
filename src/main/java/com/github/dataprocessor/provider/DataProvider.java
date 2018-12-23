package com.github.dataprocessor.provider;

import com.github.dataprocessor.slice.Slice;

import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

/**
 * 数据提供者，负责生成切片、分页获取来源数据和将分批之后的资源的处理方式，解决怎么分区，数据从哪来到哪去的问题
 *
 * @param <T> 需要处理的数据类型
 * @param <S> 分片类
 * @author huangxuyang
 * date 2018/10/27
 */
public interface DataProvider<T, S> {
    /**
     * 获取所有分片
     *
     * @return 分片
     */
    Set<Slice<S>> generateSlices();

    /**
     * 从数据源获取需要被处理的资源
     *
     * @param slice    分片
     * @param lastPage 上一页，即刚刚处理完成的这一页，如果是第一次获取则为null
     * @return 需要被处理的资源，若hashNext()返回false则认为本批次已处理完成
     * @throws Exception 获取资源时抛出的任何异常
     */
    Page<T> getResources(Slice<S> slice, Page<T> lastPage) throws Exception;

    /**
     * 创建实际处理逻辑的任务
     *
     * @param resources 本批次需要处理的资源
     * @return 实际处理逻辑的任务，注意：Callable调用后抛出异常，则认为本批次处理失败
     */
    Callable<?> createTask(List<T> resources);
}
