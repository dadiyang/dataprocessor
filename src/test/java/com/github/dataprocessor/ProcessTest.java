package com.github.dataprocessor;

import com.github.dataprocessor.provider.DataProvider;
import com.github.dataprocessor.provider.LongSliceDataProvider;
import com.github.dataprocessor.provider.Page;
import com.github.dataprocessor.slice.Slice;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.Assert.assertTrue;

/**
 * 测试正常处理流程
 *
 * @author huangxuyang
 * date 2018/10/28
 */
public class ProcessTest {
    private List<Integer> source;
    private List<Integer> target;
    private int span = 128;

    @Before
    public void setUp() throws Exception {
        int testDataSize = 1_000;
        source = new ArrayList<>(testDataSize);
        Random random = new Random();
        for (int i = 0; i < testDataSize; i++) {
            source.add(random.nextInt());
        }
        target = new CopyOnWriteArrayList<>();
    }

    @Test
    public void process() {
        DataProvider<Integer, Long> provider = new MockDataProvider();
        DefaultDataProcessor migrator = new DefaultDataProcessor<>(provider);
        migrator.setLaunchInterval(0);
        migrator.process();
        // 判断两个list完全相等
        assertTrue(source.containsAll(target));
        assertTrue(target.containsAll(source));
    }

    private class MockDataProvider extends LongSliceDataProvider<Integer> {
        private int pageSize = 100;

        public MockDataProvider() {
            super(0, source.size(), span, true);
        }

        @Override
        public Page<Integer> getResources(Slice<Long> slice, Page<Integer> lastPage) throws Exception {
            int start = slice.getBegin().intValue();
            int currentPage = 0;
            if (lastPage != null) {
                currentPage = lastPage.getCurrentPage() + 1;
            }
            start += currentPage * pageSize;
            int end = start + pageSize;
            return new Page<>(end < slice.getEnd(), source.subList(start,
                    Math.min(end, slice.getEnd().intValue())), pageSize, currentPage);
        }

        @Override
        public Callable<?> createTask(List<Integer> resources) {
            return (Callable<Object>) () -> {
                for (Integer resource : resources) {
                    int r = resource;
                    // 处理可能出现的重复数据（失败重试时会有）
                    if (!target.contains(r)) {
                        target.add(r);
                    }
                }
                return true;
            };
        }
    }
}