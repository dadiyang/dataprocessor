package com.github.dataprocessor;

import com.github.dataprocessor.provider.DataProvider;
import com.github.dataprocessor.provider.Page;
import com.github.dataprocessor.slice.Slice;

import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

/**
 * 数据处理器，继承数据处理器模板，将必须实现的三个方法委托给 DataProvider
 * <p>
 * 使用此数据处理器，必须提供 DataProvider<T, S> 接口的一个实现
 * 这个接口封装你的分片规则、分页获取来源数据方式和将分批之后的资源处理到你的目标库的方式
 * <p>
 * 换句话说，你只需要实现分片、查询来源数据和写入目标库的方式，其他的事情都有框架进行处理
 * <p>
 * 使用的时候只需要调用 process() 方法就可以开始执行处理
 *
 * @param <T> 被处理的对象类，如：商机
 * @param <S> 分片类，如：时间、id等
 * @author huangxuyang
 * @date 2018/10/27
 */
public class DefaultDataProcessor<T, S> extends DataProcessorTemplate<T, S> {
    private final DataProvider<T, S> dataProvider;

    /**
     * 必须提供的数据提供器
     *
     * @param dataProvider 数据提供器
     */
    public DefaultDataProcessor(DataProvider<T, S> dataProvider) {
        this.dataProvider = dataProvider;
    }

    /**
     * 必须提供的数据提供器，指定记录文件存放目录
     *
     * @param dataProvider         数据提供器
     * @param sliceRecorderBaseDir 切片记录器记录文件存放目录
     */
    public DefaultDataProcessor(DataProvider<T, S> dataProvider, String sliceRecorderBaseDir) {
        super(sliceRecorderBaseDir);
        this.dataProvider = dataProvider;
    }

    /**
     * 指定每批数量、同时处理的分片数
     *
     * @param numPerBatch     每批数量
     * @param slicesThreadNum 同时处理的分片数
     * @param dataProvider    数据提供器
     */
    public DefaultDataProcessor(int numPerBatch, int slicesThreadNum, DataProvider<T, S> dataProvider) {
        super(numPerBatch, slicesThreadNum);
        this.dataProvider = dataProvider;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Set<Slice<S>> generateSlices() {
        return dataProvider.generateSlices();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Page<T> getResources(Slice<S> slice, Page<T> lastPage) throws Exception {
        return dataProvider.getResources(slice, lastPage);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Callable<?> createTask(List<T> resources) {
        return dataProvider.createTask(resources);
    }

    public DataProvider<T, S> getDataProvider() {
        return dataProvider;
    }
}
