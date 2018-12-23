package com.github.dataprocessor.slice;

import java.util.Set;

/**
 * 分片的编解码器，用于序列化和反序列化
 *
 * @param <S> 分片的类型
 */
public interface SliceParser<S> {

    /**
     * 解码，即反序列化
     *
     * @param sliceString 序列化后的字符串
     * @return 时间分片
     */
    Slice<S> parse(String sliceString);

    /**
     * 反序列化集合
     *
     * @param sliceString 序列化后的字符串
     * @return 时间分片集合
     */
    Set<Slice<S>> parseSlices(String sliceString);

    /**
     * 编码，即序列化
     *
     * @param slice 时间分片
     * @return 序列化后的字符串
     */
    String serialize(Slice<S> slice);

    /**
     * 序列化分片集合
     *
     * @param slices 分片集合
     * @return 序列化后的字符串
     */
    String serialize(Set<Slice<S>> slices);
}
