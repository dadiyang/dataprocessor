package com.github.dataprocessor;

import com.github.dataprocessor.provider.Page;
import com.github.dataprocessor.slice.*;
import com.github.dataprocessor.threadpool.DefaultThreadPoolFactory;
import com.github.dataprocessor.threadpool.ThreadPoolFactory;
import com.github.dataprocessor.util.RetryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 数据处理器模板，使用此模板，需要实现
 * generateSlices、getResources、createTask 三个方法
 * <p>
 * generateSlices 用于生成分片，以根据分片进行分批处理
 * getResources   从来源处获取需要处理的数据
 * createTask     创建根据给定的分批数据进行实际处理的任务
 * <p>
 * 使用的时候只需要调用 process() 方法就开始执行处理
 *
 * @param <T> 被处理的对象类，如：商机
 * @param <S> 分片类，如：时间、id等
 * @author huangxuyang
 * @date 2018/10/26
 */
@SuppressWarnings("AlibabaAbstractClassShouldStartWithAbstractNaming")
public abstract class DataProcessorTemplate<T, S> implements DataProcessor {
    private static final Logger logger = LoggerFactory.getLogger(DataProcessorTemplate.class);
    private static final String THREAD_NAME = "processor";
    private static final int DEFAULT_SLICES_THREAD_NUM = 8;
    private static final int DEFAULT_NUM_PER_BATCH = 1000;
    /**
     * 执行状态，如果任务正在执行此值不为0
     */
    private int state;
    /**
     * 计数器
     */
    private AtomicLong counter = new AtomicLong();
    /**
     * 分片线程数，即同时进行处理的分片数
     */
    private int slicesThreadNum;
    /**
     * 分片序列化和反序列化器
     */
    private SliceParser<S> sliceParser;
    /**
     * 分片记录器
     */
    private SliceRecorder<S> sliceRecorder;
    /**
     * 每批处理的数量
     */
    private int numPerBatch;
    /**
     * 多个分片同时启动时，每个启动之间的间隔，单位毫秒
     * <p>
     * 有些查询会比较耗时，如果同时启动太多个分片，会导致数据库压力过大导致超时，建议在会给数据库造成压力的时候适当调整此参数
     */
    private long launchInterval = 3000L;
    /**
     * 线程池工厂
     */
    private ThreadPoolFactory threadPoolFactory;

    /**
     * 执行任务失败的重试次数
     */
    private int retryTime = 3;
    /**
     * 被重试的方法是否可以接受null值，若不能接受则如果方法返回null值视为失败
     */
    private boolean retryNullable = true;

    /**
     * @param threadPoolFactory 线程池工厂
     * @param sliceParser       分片的解析器
     * @param sliceRecorder     分片记录器
     * @param numPerBatch       每批处理的数量
     * @param slicesThreadNum   同时处理的分片数量
     */
    public DataProcessorTemplate(ThreadPoolFactory threadPoolFactory, SliceParser<S> sliceParser, SliceRecorder<S> sliceRecorder, int numPerBatch, int slicesThreadNum) {
        this.threadPoolFactory = threadPoolFactory;
        this.sliceParser = sliceParser;
        this.sliceRecorder = sliceRecorder;
        this.numPerBatch = numPerBatch;
        this.slicesThreadNum = slicesThreadNum;
    }

    /**
     * 指定每批数量
     *
     * @param numPerBatch 每批的数量
     */
    public DataProcessorTemplate(int numPerBatch) {
        this(new DefaultSliceParser<>(), numPerBatch, DEFAULT_SLICES_THREAD_NUM);
    }

    /**
     * 指定每批数量和分片线程数
     *
     * @param numPerBatch     每批的数量
     * @param slicesThreadNum 分片线程数
     */
    public DataProcessorTemplate(int numPerBatch, int slicesThreadNum) {
        this(new DefaultSliceParser<>(), numPerBatch, slicesThreadNum);
    }

    /**
     * 指定分片解析器、每批数量和分片线程数
     *
     * @param parser          分片解析器
     * @param numPerBatch     每批数量
     * @param slicesThreadNum 分片线程数
     */
    public DataProcessorTemplate(DefaultSliceParser<S> parser, int numPerBatch, int slicesThreadNum) {
        this(new DefaultThreadPoolFactory(), parser, new DefaultSliceRecorder<>(parser), numPerBatch, slicesThreadNum);
    }

    /**
     * 全部参数都使用默认的实现，每页5000、每批1000、同时启动8个分片
     */
    protected DataProcessorTemplate() {
        this(new DefaultSliceParser<>(), DEFAULT_NUM_PER_BATCH, DEFAULT_SLICES_THREAD_NUM);
    }

    /**
     * 指定记录文件存放目录
     *
     * @param sliceRecorderBaseDir 切片记录器记录文件存放目录
     */
    protected DataProcessorTemplate(String sliceRecorderBaseDir) {
        this(DEFAULT_NUM_PER_BATCH, DEFAULT_SLICES_THREAD_NUM);
        this.sliceParser = new DefaultSliceParser<>();
        this.sliceRecorder = new DefaultSliceRecorder<>(sliceParser, sliceRecorderBaseDir);
        this.threadPoolFactory = new DefaultThreadPoolFactory();
        this.numPerBatch = DEFAULT_NUM_PER_BATCH;
        this.slicesThreadNum = DEFAULT_SLICES_THREAD_NUM;
    }

    /**
     * 实现分片规则，处理程序将会以此方法返回的分片进行分批处理
     *
     * @return 分片
     */
    protected abstract Set<Slice<S>> generateSlices();

    /**
     * 从数据源获取需要处理的资源
     * <p>
     * 注意：不允许返回null值，若为null值则视为失败
     *
     * @param slice    分片
     * @param lastPage 上一页，即刚刚处理完成的这一页，如果是第一次获取则为null
     * @return 需要处理的资源，若hashNext()返回false则认为本批次已处理完成，null值被视为获取失败
     * @throws Exception 失败时抛出
     */
    protected abstract Page<T> getResources(Slice<S> slice, Page<T> lastPage) throws Exception;

    /**
     * 创建实际处理逻辑的任务
     *
     * @param resources 分批后的资源
     * @return 实际处理逻辑的任务，注意：Callable调用后抛出异常，则认为本批次处理失败
     */
    protected abstract Callable<?> createTask(List<T> resources);

    /**
     * 处理数据，如果有任务正在执行（state!=0），不允许调用此方法
     */
    @Override
    public boolean process() {
        runState();
        try {
            Set<Slice<S>> slices = generateSlices();
            return launchSlices(slices);
        } finally {
            state = 0;
        }
    }

    private boolean launchSlices(Set<Slice<S>> slices) {
        long start = System.currentTimeMillis();
        sliceRecorder.clearRecord();
        counter.set(0);
        Set<Slice<S>> errorSlices = launchBySliceTasks(slices);
        if (!errorSlices.isEmpty()) {
            logger.info("有{}个分片失败了，尝试重新处理失败的分片: {}", errorSlices.size(), errorSlices);
            errorSlices = launchBySliceTasks(errorSlices);
        }
        if (errorSlices.isEmpty()) {
            logger.info("数据处理任务全部完成，总量:{}, 共耗时:{}", counter.get(), (System.currentTimeMillis() - start));
            return true;
        } else {
            logger.warn("数据处理任务执行结束但有执行失败的分片{}个，总量:{}, 共耗时:{}，", errorSlices.size(), counter.get(), (System.currentTimeMillis() - start));
            logger.warn("出错的分片: " + errorSlices);
            return false;
        }
    }

    /**
     * 处理失败的分片，如果有任务正在执行（state!=0），不允许调用此方法
     *
     * @return 全部处理成功或者没有需要重新处理的错误分片则返回true
     * @throws ConcurrentModificationException 有任务正在执行时抛出
     */
    @Override
    public boolean processErrorSlices() {
        long start = System.currentTimeMillis();
        runState();
        logger.info("开始处理失败的分片");
        try {
            counter.set(0);
            Set<Slice<S>> errorSlices = sliceRecorder.getErrorSlices();
            if (errorSlices == null || errorSlices.isEmpty()) {
                logger.info("没有获取到需要重新处理的错误分片");
                return true;
            }
            // 排除掉已完成的
            Set<Slice<S>> completed = sliceRecorder.getCompletedSlices();
            errorSlices.removeAll(completed);
            if (errorSlices.isEmpty()) {
                logger.info("有获取到{}个失败的分片，但之前已全部处理完成");
                return true;
            }
            logger.info("共获取到 {} 个处理失败的分片，现在开始处理", errorSlices.size());
            Set<Slice<S>> err = launchBySliceTasks(errorSlices);
            if (err.isEmpty()) {
                logger.info("失败的分片重新处理完毕，总量: {}, 耗时: {}", errorSlices.size(), (System.currentTimeMillis() - start));
                return true;
            } else {
                logger.info("失败的分片重新处理完毕但有再次失败的分片{}个，总错误分片数量: {}, 耗时: {}", err.size(), errorSlices.size(), (System.currentTimeMillis() - start));
                logger.warn("再次出错的分片: " + err);
                return false;
            }
        } finally {
            state = 0;
        }
    }

    /**
     * 恢复上次未完成的任务（断点续传），如果有任务正在执行（state!=0），不允许调用此方法
     * 若上次没有未完成的任务，或者上次的任务没有一个切片是完成了的，则会退出
     *
     * @throws ConcurrentModificationException 有任务正在执行时抛出
     * @throws IllegalStateException           注意！没有读取到上次未完成的任务或者上次所有分片与已完成的分片无法取差集时抛出
     */
    @Override
    public boolean resumeProgress() {
        runState();
        try {
            Set<Slice<S>> allSlice = sliceRecorder.getAllSlices();
            if (allSlice == null || allSlice.isEmpty()) {
                String msg = "没有读取到上次执行的分片记录，无法恢复上次的未完成任务，请重新进行全量处理";
                logger.warn(msg);
                throw new IllegalStateException(msg);
            }
            Set<Slice<S>> completedSlice = sliceRecorder.getCompletedSlices();
            if (completedSlice == null || completedSlice.isEmpty()) {
                String msg = "没有读取已完成的分片，无法恢复上次的未完成任务，请重新进行全量处理";
                logger.warn(msg);
                throw new IllegalStateException(msg);
            }
            boolean isRemoved = allSlice.removeAll(completedSlice);
            if (isRemoved) {
                logger.info("开始恢复上次未完成的任务");
                launchSlices(allSlice);
                logger.info("恢复上次未完成的任务结束");
            } else {
                String msg = "上次记录的时间分片与已完成的分片无法取差集，请确认用于分片的类型是否实现了equals和hashCode方法，且上次的记录没有被篡改";
                logger.warn(msg);
                throw new IllegalStateException(msg);
            }
        } finally {
            state = 0;
        }
        // 全部处理完毕之后，需要重新
        logger.info("尝试处理错误数据，若没有错误数据需要处理，则为全部成功");
        return processErrorSlices();
    }

    /**
     * 根据分片启动处理任务
     *
     * @param slices 要处理的分片
     * @return 处理出错的时间分片
     */
    private Set<Slice<S>> launchBySliceTasks(Set<Slice<S>> slices) {
        if (slices == null || slices.isEmpty()) {
            logger.warn("没有需要执行的分片");
            return Collections.emptySet();
        }
        Set<Slice<S>> errorSlices = new LinkedHashSet<>();
        sliceRecorder.saveAllSlices(slices);
        try {
            logger.info("分片任务开始启动，同时开始分片数:{}, 共有{}个分片需要处理", slicesThreadNum, slices.size());
            // 如果只有一个切片，则直接处理，不再启动线程池
            if (slices.size() == 1) {
                launchSlice(errorSlices, slices.iterator().next());
                return errorSlices;
            }
            ExecutorService executor = threadPoolFactory.createThreadPool(slicesThreadNum, THREAD_NAME + "-sliceLauncher");
            for (final Slice<S> slice : slices) {
                // 处理每个分片
                if (slice != null) {
                    executor.execute(() -> launchSlice(errorSlices, slice));
                    // 错开时间执行
                    Thread.sleep(launchInterval);
                }
            }
            logger.info("分片任务启动完成，等待执行");
            executor.shutdown();
            executor.awaitTermination(7, TimeUnit.DAYS);
            logger.info("分片任务执行完成，总量: " + counter.get());
        } catch (InterruptedException e) {
            logger.error("分片任务启动发生异常", e);
            Thread.currentThread().interrupt();
        }
        return errorSlices;
    }

    /**
     * 启动分片任务并记录处理结果
     *
     * @param errorSlicesCollector 如果分片处理失败则把该分片添加到这个集合中
     * @param slice                本次要处理的分片
     */
    private void launchSlice(Set<Slice<S>> errorSlicesCollector, Slice<S> slice) {
        boolean rs = false;
        try {
            rs = processBySlice(slice);
        } catch (InterruptedException e) {
            logger.error("处理批次发生异常, 分片: " + slice, e);
        }
        if (rs) {
            logger.info("分片任务 {} 完成, 当前处理总数: {}", slice.toString(), counter.get());
            sliceRecorder.saveCompletedSlice(slice);
        } else {
            logger.info("当前时间分片处理失败: " + slice);
            sliceRecorder.saveErrorSlice(slice);
            errorSlicesCollector.add(slice);
        }
    }

    /**
     * 建立线程池启动处理单个时间分片的任务
     *
     * @param slice 需要处理的时间分片
     * @return 是否成功
     * @throws InterruptedException 执行中断
     */
    private boolean processBySlice(final Slice<S> slice) throws InterruptedException {
        long start = System.currentTimeMillis();
        long count = 0L;
        List<Future> allFutures = new LinkedList<>();
        ExecutorService taskPool = null;
        Page<T> currentPage;
        Page<T> lastResource = null;
        do {
            logger.info("从来源获取需要处理的资源开始");
            final Page<T> lastPage = lastResource;
            try {
                currentPage = RetryUtil.retryCall(() -> getResources(slice, lastPage), retryTime, false);
                if (currentPage == null) {
                    logger.info("分页获取到null值，认为本分片处理失败:" + slice);
                    return false;
                }
            } catch (Exception e) {
                logger.error("分片任务执行有异常，本分片处理失败: " + slice, e);
                return false;
            }

            List<T> resources = currentPage.getData();
            if (resources == null || resources.size() <= 0) {
                // 没有数据则退出循环
                logger.info("查无数据，认为本批次数据已全部获取完成");
                break;
            }
            logger.info("从来源获取需要处理的资源结束，数据量: " + resources.size());
            // 第一次查询到的资源数量比要求的少且只有一批，直接单线程一次处理
            if (useSingleThread(count, currentPage.isHasNext(), resources.size())) {
                try {
                    logger.debug("使用单线线程执行分批导入任务, count:{}, hasNext:{}, srcSize:{}", count, currentPage.isHasNext(), resources.size());
                    Callable<?> callable = createTask(resources);
                    RetryUtil.retryCall(callable, retryTime, retryNullable);
                } catch (Exception e) {
                    logger.error("分片任务执行有异常，本分片处理失败: " + slice, e);
                    return false;
                }
            } else {
                // 只在需要的时候才初始化线程池
                if (taskPool == null) {
                    // 控制总体线程数不超过理想值，上面useSingleThread已确保poolSize>1
                    int poolSize = desiredThreadNum() / slicesThreadNum;
                    logger.info("创建分批处理线程池,线程数量: " + poolSize);
                    taskPool = threadPoolFactory.createThreadPool(poolSize, THREAD_NAME + "-" + slice.getBegin() + "-" + slice.getEnd());
                }
                // 切割并启动处理任务
                List<Future> futures = execTask(taskPool, resources);
                // 将执行处理任务返回的Future记录下来以确认是否处理成功
                allFutures.addAll(futures);
            }
            lastResource = currentPage;
            count += resources.size();
        } while (currentPage.isHasNext());
        logger.info("本批次 {} 任务同步启动，等待执行", slice);
        if (taskPool != null) {
            taskPool.shutdown();
            taskPool.awaitTermination(1, TimeUnit.HOURS);
        }
        // 只要有一个执行失败，则认为本分片任务执行失败
        // 这是为性能和实际情况考虑的一个取舍，实际情况中出错的概率较小，而且前面加上重试机制，失败的可能性大大降低
        // 如果每批次都等待再继续下一个批次，则会降低效率；
        // 如果这里再加一个失败重试机制，复杂性增加，而且如果前面已多次重试失败，这里再重试意义也不大
        for (Future future : allFutures) {
            try {
                Object obj = future.get();
                // 返回值为null则说明执行失败
                if (obj == null || Objects.equals(obj, false)) {
                    return false;
                }
            } catch (Exception e) {
                logger.error("分片任务执行有异常，本分片处理失败: " + slice, e);
                return false;
            }
        }
        logger.info("批次 {} 处理完成，共处理 {} 条数据，耗时: {}", slice, count, (System.currentTimeMillis() - start));
        counter.addAndGet(count);
        return true;
    }

    /**
     * 根据本次资源创建Task提交到给定的线程池中
     * <p>
     * 如果本次资源的量超过每批需要处理的量则进行切分
     *
     * @param taskPool  线程池
     * @param resources 需要被处理的资源
     * @return 任务提交到线程池后返回的 Future 类
     */
    private List<Future> execTask(ExecutorService taskPool, List<T> resources) {
        // 资源的数量比每批需要处理的数据少或相同则直接添加到任务队列
        if (resources.size() <= numPerBatch) {
            return Collections.singletonList(submitRetryTask(taskPool, createTask(resources)));
        } else {
            // 否则将获取到的商机进行分批
            List<Future> futures = new LinkedList<>();
            for (int i = 0; i < resources.size(); i += numPerBatch) {
                int toIndex = i + numPerBatch;
                List<T> subList = resources.subList(i, toIndex > resources.size() ? resources.size() : toIndex);
                futures.add(submitRetryTask(taskPool, createTask(subList)));
            }
            return futures;
        }
    }

    /**
     * 提交出错会重试的任务，就是将 callable 包了一层重试机制
     *
     * @param taskPool 线程池
     * @param callable 具体调用的方法
     * @return 任务提交到线程池后返回的 Future 类
     */
    private Future submitRetryTask(ExecutorService taskPool, Callable<?> callable) {
        return taskPool.submit((Callable<Object>) () -> RetryUtil.retryCall(callable, retryTime, retryNullable));
    }

    /**
     * 进入运行状态
     *
     * @throws ConcurrentModificationException 如果 state 不等于0则表示有任务正在执行，抛出非法状态异常
     */
    private void runState() {
        ensureState();
        state = 1;
    }

    /**
     * 判断是否使用单线程，可以启动的线程数 <=1 或者一次性可以全部导完就使用单线程
     *
     * @param count       当前处理的总量
     * @param hasNextPage 是否有下一页
     * @param srcSize     本次需要处理的资源量
     * @return 是否使用单线程
     */
    private boolean useSingleThread(long count, boolean hasNextPage, int srcSize) {
        return (desiredThreadNum() / slicesThreadNum <= 1)
                || (count == 0 && !hasNextPage && srcSize <= numPerBatch);
    }

    /**
     * 理想的线程数，使用 2倍cpu核心数+1
     */
    private int desiredThreadNum() {
        return Runtime.getRuntime().availableProcessors() * 2 + 1;
    }

    public void setSlicesThreadNum(int slicesThreadNum) {
        requirePositive(slicesThreadNum, "分片任务执行线程数必须大于0, slicesThreadNum:");
        ensureState();
        this.slicesThreadNum = slicesThreadNum;
    }

    public void setSliceRecorder(SliceRecorder<S> sliceRecorder) {
        ensureState();
        this.sliceRecorder = sliceRecorder;
    }

    public void setNumPerBatch(int numPerBatch) {
        requirePositive(numPerBatch, "每批次的数量必须大于0，numPerBatch:");
        ensureState();
        this.numPerBatch = numPerBatch;
    }

    private void requirePositive(int num, String msg) {
        if (num <= 0) {
            throw new IllegalArgumentException(msg + num);
        }
    }

    private void requireNotNegative(long num, String msg) {
        if (num < 0) {
            throw new IllegalArgumentException(msg + num);
        }
    }

    public void setSliceParser(SliceParser<S> sliceParser) {
        ensureState();
        this.sliceParser = sliceParser;
    }

    /**
     * 设置间隔时间
     *
     * @param launchInterval 间隔时间，单位：毫秒
     * @throws IllegalArgumentException 给定时间小于0时抛出
     */
    public void setLaunchInterval(long launchInterval) {
        requireNotNegative(launchInterval, "启动间隔不能为负数，launchInterval:");
        ensureState();
        this.launchInterval = launchInterval;
    }

    public void setThreadPoolFactory(ThreadPoolFactory threadPoolFactory) {
        if (threadPoolFactory == null) {
            throw new NullPointerException("线程池工厂不能为空");
        }
        ensureState();
        this.threadPoolFactory = threadPoolFactory;
    }

    public void setRetryTime(int retryTime) {
        requireNotNegative(retryTime, "重试次数不能为负数: retryTime:");
        ensureState();
        this.retryTime = retryTime;
    }

    public void setRetryNullable(boolean retryNullable) {
        ensureState();
        this.retryNullable = retryNullable;
    }

    /**
     * 判断当前执行状态
     *
     * @throws ConcurrentModificationException 如果 state 不等于0则表示有任务正在执行，抛出此异常
     */
    private void ensureState() {
        if (state != 0) {
            throw new ConcurrentModificationException("当前有任务正在执行");
        }
    }

    public long getLaunchInterval() {
        return launchInterval;
    }

    public int getNumPerBatch() {
        return numPerBatch;
    }

    public SliceRecorder<S> getSliceRecorder() {
        return sliceRecorder;
    }

    public int getSlicesThreadNum() {
        return slicesThreadNum;
    }

    public SliceParser<S> getSliceParser() {
        return sliceParser;
    }

    public ThreadPoolFactory getThreadPoolFactory() {
        return threadPoolFactory;
    }

    public int getRetryTime() {
        return retryTime;
    }

    public boolean isRetryNullable() {
        return retryNullable;
    }

}
