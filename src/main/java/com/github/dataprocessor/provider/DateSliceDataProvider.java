package com.github.dataprocessor.provider;

import com.github.dataprocessor.slice.Slice;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * 根据时间分片的数据提供者
 *
 * @param <T> 数据类型
 * @author huangxuyang
 * date 2018/10/27
 */
public abstract class DateSliceDataProvider<T> implements DataProvider<T, Date> {
    private Date min;
    private Date max;
    private boolean ordered;
    private long spanMs;

    /**
     * 无参构造器，使用此构造器，必须在真正使用的时候调用setter方法设置日期的最大和最小值
     */
    public DateSliceDataProvider() {
    }

    /**
     * 给定一个日期的最小值（开始时间）、最大值（结束时间）和间隔天数
     * <p>
     * 默认使用返回的分片集合为无序集
     *
     * @param min     最小值（开始时间）
     * @param max     最大值（结束时间）
     * @param spanDay 时间间隔（单位：天）必须大于0
     * @throws IllegalArgumentException 最小时间不小于最大时间或者时间间隔小于等于0时抛出
     */
    protected DateSliceDataProvider(Date min, Date max, int spanDay) {
        this(min, max, spanDay * 86_400_000L, false);
    }

    /**
     * 给定一个日期的最小值（开始时间）、最大值（结束时间）和间隔天数
     *
     * @param min     最小值（开始时间）
     * @param max     最大值（结束时间）
     * @param spanDay 时间间隔（单位：天）必须大于0
     * @param ordered 是否支持按顺序遍历分片
     * @throws IllegalArgumentException 最小时间不小于最大时间或者时间间隔小于等于0时抛出
     */
    protected DateSliceDataProvider(Date min, Date max, int spanDay, boolean ordered) {
        this(min, max, spanDay * 86_400_000L, ordered);
    }

    /**
     * 给定一个日期的最小值（开始时间）和最大值（结束时间）和间隔毫秒数
     * <p>
     * 默认使用返回的分片集合为无序集
     *
     * @param min    最小值（开始时间）
     * @param max    最大值（结束时间）
     * @param spanMs 时间间隔（单位：毫秒）
     */
    protected DateSliceDataProvider(Date min, Date max, long spanMs) {
        this(min, max, spanMs, false);
    }

    /**
     * 给定一个日期的最小值（开始时间）和最大值（结束时间）和间隔毫秒数
     *
     * @param min     最小值（开始时间）
     * @param max     最大值（结束时间）
     * @param spanMs  时间间隔（单位：毫秒）
     * @param ordered 是否支持按顺序遍历分片
     */
    protected DateSliceDataProvider(Date min, Date max, long spanMs, boolean ordered) {
        if (spanMs <= 0) {
            throw new IllegalArgumentException("时间间隔必须大于0: " + spanMs);
        }
        if (min.getTime() >= max.getTime()) {
            throw new IllegalArgumentException("最小时间必须小于最大时间, min: " + min + ", max:" + max + ", spanMs:" + spanMs);
        }
        this.min = min;
        this.max = max;
        this.spanMs = spanMs;
        this.ordered = ordered;
    }

    @Override
    public Set<Slice<Date>> generateSlices() {
        if (min == null || max == null) {
            throw new IllegalStateException("日期的最大和最小值不能为空, min:" + min + ", max:" + max);
        }
        if (max.before(min)) {
            throw new IllegalStateException("日期的最大值必须大于最小值, min:" + min + ", max:" + max);
        }
        if (spanMs <= 0) {
            throw new IllegalStateException("时间间隔必须大于0, spanMs:" + spanMs);
        }
        long start = min.getTime();
        long end = nextEnd(start, spanMs);
        Set<Slice<Date>> slices = ordered ? new LinkedHashSet<>() : new HashSet<>();
        while (end <= max.getTime()) {
            slices.add(new Slice<>(new Date(start), new Date(end)));
            start = end;
            end = nextEnd(end, spanMs);
        }
        if (start != max.getTime()) {
            slices.add(new Slice<>(new Date(start), max));
        }
        return slices;
    }

    /**
     * 根据自己的需要调整每个分片的步长，单位都是毫秒
     * <p>
     * 有些情况下可能需要切割的分片步长是不等的，这种情况下就可以通过重写此方法来做
     *
     * @param end    上次的结束时间的毫秒数，第一次获取则为start的值
     * @param spanMs 间隔
     * @return 下一个结束点
     */
    protected long nextEnd(long end, long spanMs) {
        return end + spanMs;
    }

    /**
     * 日期类转为LocalDateTime，有些子类可能会需要用到
     *
     * @param date 日期
     * @return LocalDateTime
     */
    protected LocalDateTime date2LocalDateTime(Date date) {
        Instant instant = date.toInstant();
        ZoneId zoneId = ZoneId.systemDefault();
        return instant.atZone(zoneId).toLocalDateTime();
    }

    public Date getMin() {
        return min;
    }

    public void setMin(Date min) {
        this.min = min;
    }

    public Date getMax() {
        return max;
    }

    public void setMax(Date max) {
        this.max = max;
    }

    public boolean isOrdered() {
        return ordered;
    }

    public void setOrdered(boolean ordered) {
        this.ordered = ordered;
    }

    public long getSpanMs() {
        return spanMs;
    }

    public void setSpanMs(long spanMs) {
        this.spanMs = spanMs;
    }
}
