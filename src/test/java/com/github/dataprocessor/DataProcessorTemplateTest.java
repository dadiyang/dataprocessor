package com.github.dataprocessor;

import com.github.dataprocessor.provider.Page;
import com.github.dataprocessor.slice.Slice;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.Callable;

public class DataProcessorTemplateTest {

    @Test
    public void testHaveNoSlice() {
        MockDataProcessorTemplate<Object, LocalDateTime> migrator = new MockDataProcessorTemplate<Object, LocalDateTime>();
        Set<Slice<LocalDateTime>> slices = new HashSet<>();
        migrator.process();
        migrator.slices = slices;
        migrator.process();
    }

    @Test
    public void testGetResourceException() {
        MockDataProcessorTemplate<Object, LocalDateTime> migrator = new MockDataProcessorTemplate<Object, LocalDateTime>();
        migrator.setLaunchInterval(0);
        Set<Slice<LocalDateTime>> slices = new HashSet<>();
        LocalDateTime start = LocalDateTime.now().minusYears(2);
        for (int i = 0; i < 8; i++) {
            LocalDateTime end = start.plusDays(15);
            slices.add(new Slice<>(start, end));
            start = end;
        }
        migrator.slices = slices;
        migrator.process();
    }

    @Test
    public void testCreateNullTask() {
        MockDataProcessorTemplate<Object, LocalDateTime> migrator = new MockDataProcessorTemplate<>();
        migrator.slices = Collections.singleton(new Slice<>(LocalDateTime.now(), LocalDateTime.now().plusDays(1)));
        Page<Object> page = new Page<>(true, Arrays.asList("1234567", "867", "dfasdfa"), 1, 1000);
        page.setHasNext(true);
        page.setCurrentPage(1);
        page.setData(Arrays.asList("1234567", "867", "dfasdfa"));
        page.setPageSize(1000);
        migrator.process();
    }

    @Test
    public void processErrorSlices() {
    }

    @Test
    public void resumeProgress() {
    }


    private class MockDataProcessorTemplate<T, S> extends DataProcessorTemplate<T, S> {
        private Set<Slice<S>> slices;
        private Page<T> resources;

        public MockDataProcessorTemplate() {
            super(500, 8);
        }

        @Override
        protected Set<Slice<S>> generateSlices() {
            return slices;
        }

        @Override
        protected Page<T> getResources(Slice<S> slice, Page<T> lastPage) throws Exception {
            if (resources == null) {
                throw new Exception("测试获取资源出错的场景");
            }
            return resources;
        }

        @Override
        protected Callable<?> createTask(List<T> resources) {
            return null;
        }
    }
}