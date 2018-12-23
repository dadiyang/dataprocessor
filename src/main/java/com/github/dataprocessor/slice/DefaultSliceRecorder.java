package com.github.dataprocessor.slice;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 默认的分片记录器，使用文件存储
 *
 * @param <S> 分片类型
 * @author huangxuyang
 * @date 2018/10/26
 */
public class DefaultSliceRecorder<S> implements SliceRecorder<S> {
    private static final Logger logger = LoggerFactory.getLogger(DefaultSliceParser.class);
    private final String fileAllSlices;
    private final String fileCompletedSlice;
    private final String fileErrorSlice;
    private SliceParser<S> sliceParser;
    private final String baseDir;
    private final int MAX_HISTORY = 10;
    private final String HISTORY_FILE_PREFIX = "hist_";

    public DefaultSliceRecorder(SliceParser<S> sliceParser) {
        this(sliceParser, "");
    }

    /**
     * 指定分片解析器和分片记录文件保存的位置
     *
     * @param sliceParser 分片解析器
     * @param dir         记录文件存放的位置
     */
    public DefaultSliceRecorder(SliceParser<S> sliceParser, String dir) {
        this.sliceParser = sliceParser;
        if (dir == null || dir.trim().isEmpty()) {
            dir = "";
        }
        if (!dir.trim().isEmpty() && !dir.endsWith(File.separator)) {
            dir += File.separator;
        }
        baseDir = dir;
        String infoDir = dir + "processInfo" + File.separator;
        this.fileAllSlices = infoDir + "allSlices.txt";
        this.fileCompletedSlice = infoDir + "completedSlices.txt";
        this.fileErrorSlice = infoDir + "errorSlice.txt";
    }


    @Override
    public void saveErrorSlice(Slice<S> slice) {
        synchronized (fileErrorSlice) {
            append(slice, fileErrorSlice);
        }
    }

    @Override
    public void saveCompletedSlice(Slice<S> slice) {
        synchronized (fileCompletedSlice) {
            append(slice, fileCompletedSlice);
        }
    }

    @Override
    public void saveAllSlices(Set<Slice<S>> slices) {
        synchronized (fileAllSlices) {
            ensureDirExists(fileAllSlices);
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(new File(fileAllSlices)))) {
                writer.write(sliceParser.serialize(slices));
            } catch (IOException e) {
                throw new RuntimeException("保存所有分片记录发生异常", e);
            }
        }
    }

    @Override
    public Set<Slice<S>> getErrorSlices() {
        synchronized (fileErrorSlice) {
            return readSlices(fileErrorSlice);
        }
    }

    @Override
    public Set<Slice<S>> getAllSlices() {
        synchronized (fileAllSlices) {
            File file = new File(fileAllSlices);
            if (!file.exists()) {
                return Collections.emptySet();
            }
            try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                // 全部分片的，只有一行记录
                String line = reader.readLine();
                if (line != null) {
                    return sliceParser.parseSlices(line);
                }
            } catch (IOException e) {
                throw new RuntimeException("保存所有分片记录发生异常", e);
            }
            return Collections.emptySet();
        }
    }

    @Override
    public Set<Slice<S>> getCompletedSlices() {
        synchronized (fileCompletedSlice) {
            return readSlices(fileCompletedSlice);
        }
    }

    @Override
    public void clearRecord() {
        logger.info("清理分片历史记录");
        synchronized (DefaultSliceParser.class) {
            String historyFolder = baseDir + "processHistory";
            String folder = historyFolder + File.separator + HISTORY_FILE_PREFIX + new SimpleDateFormat("yyyy-MM-dd-HH_mm_ss").format(new Date());
            File folderFile = new File(folder);
            folderFile.mkdirs();
            // 只要有一个成功就保留目标文件夹
            boolean rs = moveTo(fileAllSlices, folder);
            rs = moveTo(fileCompletedSlice, folder) || rs;
            rs = moveTo(fileErrorSlice, folder) || rs;
            // 一个转移成功都没有，则把文件夹也删掉
            if (!rs) {
                logger.info("没有分片历史记录需要清理");
                folderFile.deleteOnExit();
            }
            clearHistory(historyFolder);
        }
    }

    /**
     * 清理多余的历史 只保存 {@link #MAX_HISTORY} 个历史记录
     *
     * @param historyFolder 历史保存的路径
     */
    private void clearHistory(String historyFolder) {
        File historyFiles = new File(historyFolder);
        if (!historyFiles.exists() || !historyFiles.isDirectory()) {
            return;
        }
        File[] files = historyFiles.listFiles();
        if (files == null || files.length <= MAX_HISTORY) {
            return;
        }
        // 根据最新修改时候排序，安全起见，只有以 HISTORY_FILE_PREFIX 开关的目录才会被删除
        List<File> fs = Arrays.stream(files)
                .filter(file -> file.getName().startsWith(HISTORY_FILE_PREFIX))
                .sorted(Comparator.comparingLong(File::lastModified)).collect(Collectors.toList());
        if (fs.size() > MAX_HISTORY) {
            List<File> toBeDel = fs.subList(0, fs.size() - MAX_HISTORY);
            for (File file : toBeDel) {
                deleteDir(file);
                logger.info("清理历史记录:" + file.getAbsoluteFile());
            }
        }
    }

    private void deleteDir(File path) {
        if (!path.exists()) {
            return;
        }
        if (path.isFile()) {
            path.delete();
            return;
        }
        File[] files = path.listFiles();
        for (int i = 0; files != null && i < files.length; i++) {
            deleteDir(files[i]);
        }
        path.delete();
    }

    private boolean moveTo(String file, String folder) {
        File all = new File(file);
        if (all.exists()) {
            logger.info("将文件 {} 移动到 {}", all.getAbsoluteFile(), folder);
            return all.renameTo(new File(folder + File.separator + all.getName()));
        }
        return false;
    }

    private Set<Slice<S>> readSlices(String fileName) {
        File file = new File(fileName);
        if (!file.exists()) {
            return Collections.emptySet();
        }
        try (BufferedReader reader = new BufferedReader(new FileReader(new File(fileName)))) {
            Set<Slice<S>> slices = new HashSet<>();
            // 按行读取，生行一个分片
            String line = reader.readLine();
            while (line != null) {
                if (!line.isEmpty()) {
                    Slice<S> slice = sliceParser.parse(line);
                    slices.add(slice);
                    line = reader.readLine();
                }
            }
            return slices;
        } catch (IOException e) {
            throw new RuntimeException("保存所有分片记录发生异常");
        }
    }

    private void ensureDirExists(String fileName) {
        File file = new File(fileName);
        if (!file.exists()) {
            file.getParentFile().mkdirs();
        }
    }

    private void append(Slice<S> slice, String fileName) {
        ensureDirExists(fileName);
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(new File(fileName), true))) {
            writer.append(sliceParser.serialize(slice)).append(System.lineSeparator());
        } catch (IOException e) {
            throw new RuntimeException("保存错误记录发生异常");
        }
    }
}
