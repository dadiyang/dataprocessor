package com.github.dataprocessor.provider;

import com.alibaba.fastjson.JSON;
import com.github.dataprocessor.slice.Slice;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;
import java.util.concurrent.Callable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class DateSliceDataProviderTest {
    private static final long DAY_MILLI = 86_400_000L;
    private long min;
    private long max;
    private long spanMs;
    private boolean expectedException;
    private boolean ordered;

    public DateSliceDataProviderTest(long min, long max, long spanMs, boolean ordered, boolean expectedException) {
        this.min = min;
        this.max = max;
        this.spanMs = spanMs;
        this.ordered = ordered;
        this.expectedException = expectedException;
    }

    @Parameterized.Parameters
    public static Collection<?> params() {
        List<Object[]> paramsList = new LinkedList<>();
        long min = System.currentTimeMillis() - 360 * DAY_MILLI;
        long max = System.currentTimeMillis();
        long spanMs = 15 * DAY_MILLI;
        // 正常情况，无序
        paramsList.add(new Object[]{min, max, spanMs, true, false});
        // 正常情况，有序
        paramsList.add(new Object[]{min, max, spanMs, true, true});
        // 每3秒一个切片
        paramsList.add(new Object[]{min, min + 12_000L, 3_000L, false, false});
        // 测试非法参数
        paramsList.add(new Object[]{min, max, 0, false, true});
        paramsList.add(new Object[]{min, 0, spanMs, false, true});
        paramsList.add(new Object[]{0, max, spanMs, false, true});
        paramsList.add(new Object[]{max, min, spanMs, false, true});
        return paramsList;
    }


    @Test
    public void paramTest() {
        try {
            System.out.println("min:" + min + ", max:" + max + ", spanMs:" + spanMs + ", ordered:" + ordered + ", expectedException:" + expectedException);
            DateSliceDataProvider<Object> dateSliceDataProvider = new DateSliceDataProvider<Object>(new Date(min), new Date(max), spanMs, ordered) {
                @Override
                public Page<Object> getResources(Slice<Date> slice, Page<Object> lastResource) throws Exception {
                    return null;
                }

                @Override
                public Callable<?> createTask(List<Object> resources) {
                    return null;
                }
            };
            Set<Slice<Date>> slices = dateSliceDataProvider.generateSlices();
            System.out.println(JSON.toJSONStringWithDateFormat(slices, "yyyy-MM-dd HH:mm:ss"));
            long last = Long.MIN_VALUE;
            for (Slice<Date> slice : slices) {
                assertTrue(slice.getBegin().before(slice.getEnd()));
                if (ordered) {
                    if (last != Long.MIN_VALUE) {
                        assertTrue("要求切片有序", slice.getBegin().getTime() >= last);
                    }
                    last = slice.getEnd().getTime();
                }
            }
            assertEquals((int) Math.ceil((max - min) / (double) spanMs), slices.size());
        } catch (Exception e) {
            e.printStackTrace();
            // 如果期望有异常则通过
            assertTrue(expectedException);
        }
    }

    @Test
    public void testSliceByDay() {
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.YEAR, -1);
        cal.add(Calendar.DAY_OF_MONTH, 5);
        Date begin = cal.getTime();
        Date end = new Date();
        int spanDay = 30;
        DateSliceDataProvider<Object> dateSliceDataProvider = new DateSliceDataProvider<Object>(begin, end, spanDay) {
            @Override
            public Page<Object> getResources(Slice<Date> slice, Page<Object> lastResource) throws Exception {
                return null;
            }

            @Override
            public Callable<?> createTask(List<Object> resources) {
                return null;
            }
        };
        Set<Slice<Date>> slices = dateSliceDataProvider.generateSlices();
        System.out.println(JSON.toJSONStringWithDateFormat(slices, "yyyy-MM-dd HH:mm:ss"));
        assertEquals((int) Math.ceil((end.getTime() - begin.getTime()) / (double) (spanDay * 8_6400_000L)), slices.size());
    }

}