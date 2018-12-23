package com.github.dataprocessor.slice;

import java.util.Objects;

/**
 * 分片
 *
 * @param <S> 分片的类型，注意：这个类型必须实现 equals和hashCode方法，否则断点续传功能无法使用
 * @author huangxuyang
 * date 2018/10/26
 */
public class Slice<S> {
    private S begin;
    private S end;

    public Slice() {
    }

    public Slice(S begin, S end) {
        this.begin = begin;
        this.end = end;
    }

    public S getBegin() {
        return begin;
    }

    public void setBegin(S begin) {
        this.begin = begin;
    }

    public S getEnd() {
        return end;
    }

    public void setEnd(S end) {
        this.end = end;
    }

    /**
     * begin 和 end 都相等才认为相等
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Slice<?> slice = (Slice<?>) o;
        return Objects.equals(begin, slice.begin) &&
                Objects.equals(end, slice.end);
    }

    @Override
    public int hashCode() {
        return Objects.hash(begin, end);
    }

    @Override
    public String toString() {
        return begin + "-" + end;
    }
}
