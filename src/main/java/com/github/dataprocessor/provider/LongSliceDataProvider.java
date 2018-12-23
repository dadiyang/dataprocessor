package com.github.dataprocessor.provider;

import com.github.dataprocessor.slice.Slice;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * 基于Long类型的分片，如果按照 id 进行分片可以使用此抽象类
 *
 * @param <T> 数据类型
 * @author huangxuyang
 * @date 2018/10/27
 */
public abstract class LongSliceDataProvider<T> implements DataProvider<T, Long> {
    private long min;
    private long max;
    private boolean ordered;
    private long span;

    /**
     * 无参构造器，使用此构造器，必须在真正使用的时候调用setter方法设置最大和最小值
     */
    public LongSliceDataProvider() {
    }

    /**
     * 指定最大值、最小值和间隔，默认使用无序集
     *
     * @param min  最小值
     * @param max  最大值
     * @param span 间隔
     */
    public LongSliceDataProvider(long min, long max, long span) {
        this(min, max, span, false);
    }

    /**
     * 指定最大值、最小值、间隔和是否有序
     *
     * @param min     最小值
     * @param max     最大值
     * @param span    间隔
     * @param ordered 是否使用有序集
     */
    protected LongSliceDataProvider(long min, long max, long span, boolean ordered) {
        this.ordered = ordered;
        if (min >= max) {
            throw new IllegalArgumentException("最小值必须小于最大值, min:" + min + ", max:" + max + ", span:" + span);
        }
        if (span <= 0) {
            throw new IllegalArgumentException("间隔必须大于0, min:" + min + ", max:" + max + ", span:" + span);
        }
        this.min = min;
        this.max = max;
        this.span = span;
    }

    @Override
    public Set<Slice<Long>> generateSlices() {
        Set<Slice<Long>> slices = ordered ? new LinkedHashSet<>() : new HashSet<>();
        long start = min;
        long end = nextEnd(min, span);
        while (end <= max) {
            slices.add(new Slice<>(start, end));
            start = end;
            end = nextEnd(end, span);
        }
        if (start != max) {
            slices.add(new Slice<>(start, max));
        }
        return slices;
    }

    public long getMin() {
        return min;
    }

    public void setMin(long min) {
        this.min = min;
    }

    public long getMax() {
        return max;
    }

    public void setMax(long max) {
        this.max = max;
    }

    public boolean isOrdered() {
        return ordered;
    }

    public void setOrdered(boolean ordered) {
        this.ordered = ordered;
    }

    public long getSpan() {
        return span;
    }

    public void setSpan(long span) {
        this.span = span;
    }

    /**
     * 根据自己的需要调整每个分片的步长
     * <p>
     * 有些情况下可能需要切割的分片步长是不等的，这种情况下就可以通过重写此方法来做
     *
     * @param end  上次的结束，第一次获取则为start的值
     * @param span 间隔
     * @return 下一个结束点
     */
    protected long nextEnd(long end, long span) {
        return end + span;
    }
}
