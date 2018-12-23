package com.github.dataprocessor.slice;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * 分片解析器的默认实现，使用JSON序列化
 * <p>
 * 因为JSON序列化无法保存实际的类型，因此在序列化的时候，前面把分片的类型先记录下来
 * <p>
 * 虽然SerializerFeature.WriteClassName也能做到，但是需要添加一些额外的配置，而且如果是非常规类型，如LocalDateTime则不支持
 *
 * @param <S> 分片的类型
 * @author huangxuyang
 * date 2018/10/26
 */
public class DefaultSliceParser<S> implements SliceParser<S> {
    private static final String TYPE_SEPARATOR = "__";

    @Override
    public Slice<S> parse(String sliceString) {
        String[] strings = splitString(sliceString);
        try {
            Class<?> clazz = Class.forName(strings[0]);
            // 防止内容中出现与 TYPE_SEPARATOR 相同的内容
            return getSlice(String.join(TYPE_SEPARATOR, Arrays.asList(strings).subList(1, strings.length)), clazz);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("反序列化失败，类型错误" + strings[0], e);
        }
    }

    @Override
    public Set<Slice<S>> parseSlices(String sliceString) {
        String[] strings = splitString(sliceString);
        try {
            Set<Slice<S>> slices = new HashSet<>();
            Class<?> clazz = Class.forName(strings[0]);
            JSONArray arr = JSON.parseArray(String.join(TYPE_SEPARATOR, Arrays.asList(strings).subList(1, strings.length)));
            for (Object o : arr) {
                slices.add(getSlice(JSON.toJSONString(o), clazz));
            }
            return slices;
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("反序列化失败，类型错误" + strings[0], e);
        }
    }

    @Override
    public String serialize(Slice<S> slice) {
        String typeName = getType(slice);
        return typeName + TYPE_SEPARATOR + JSON.toJSONStringWithDateFormat(slice, "yyyy-MM-dd'T'HH:mm:ss.SSS");
    }


    @Override
    public String serialize(Set<Slice<S>> slices) {
        if (slices == null || slices.size() <= 0) {
            throw new IllegalArgumentException("slices切片集不能为空");
        }
        String typeName = getType(slices.iterator().next());
        return typeName + TYPE_SEPARATOR + JSON.toJSONStringWithDateFormat(slices, "yyyy-MM-dd'T'HH:mm:ss.SSS");
    }

    private String[] splitString(String sliceString) {
        String[] strings = sliceString.split(TYPE_SEPARATOR);
        if (strings.length < 2) {
            throw new IllegalStateException("反序列化失败, 字符串中需要包含切片类型:" + sliceString);
        }
        return strings;
    }

    private Slice<S> getSlice(String sliceString, Type clazz) {
        JSONObject json = JSON.parseObject(sliceString);
        S begin = json.getObject("begin", clazz);
        S end = json.getObject("end", clazz);
        return new Slice<>(begin, end);
    }

    private String getType(Slice<S> slice) {
        if (slice.getBegin() == null || slice.getEnd() == null) {
            throw new IllegalArgumentException("slice对象的开始和结束时间不能都为null");
        }
        S s = slice.getBegin();
        if (slice.getBegin() == null) {
            s = slice.getEnd();
        }
        return s.getClass().getName();
    }
}
