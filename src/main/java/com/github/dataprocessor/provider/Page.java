package com.github.dataprocessor.provider;

import java.util.List;

/**
 * 数据分页信息
 *
 * @param <T> 数据类型
 * @author huangxuyang
 * date 2018/10/27
 */
public class Page<T> {
    private int currentPage;
    private int pageSize;
    private boolean hasNext;
    private List<T> data;

    /**
     * @param hasNext 是否还有下一页，这个属性是必须的，如果true则会继续，否则认为分片任务结束
     * @param data    本页数据
     */
    public Page(boolean hasNext, List<T> data) {
        this.hasNext = hasNext;
        this.data = data;
    }

    /**
     * @param hasNext  是否还有下一页，这个属性是必须的，如果true则会继续，否则认为分片任务结束
     * @param data     本页数据
     * @param pageSize 分页大小
     */
    public Page(boolean hasNext, List<T> data, int pageSize) {
        this.hasNext = hasNext;
        this.data = data;
        this.pageSize = pageSize;
    }

    /**
     * @param hasNext     是否还有下一页，这个属性是必须的，如果true则会继续，否则认为分片任务结束
     * @param data        本页数据
     * @param pageSize    分页大小
     * @param currentPage 当前页码，非必填
     */
    public Page(boolean hasNext, List<T> data, int pageSize, int currentPage) {
        this.currentPage = currentPage;
        this.pageSize = pageSize;
        this.hasNext = hasNext;
        this.data = data;
    }

    public int getCurrentPage() {
        return currentPage;
    }

    public void setCurrentPage(int currentPage) {
        this.currentPage = currentPage;
    }

    public int getPageSize() {
        return pageSize;
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    public boolean isHasNext() {
        return hasNext;
    }

    public void setHasNext(boolean hasNext) {
        this.hasNext = hasNext;
    }

    public List<T> getData() {
        return data;
    }

    public void setData(List<T> data) {
        this.data = data;
    }
}
