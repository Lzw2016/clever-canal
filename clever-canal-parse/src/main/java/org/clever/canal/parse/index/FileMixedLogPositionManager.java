package org.clever.canal.parse.index;

import org.apache.commons.io.FileUtils;
import org.clever.canal.common.utils.JsonUtils;
import org.clever.canal.common.utils.MigrateMap;
import org.clever.canal.meta.exception.CanalMetaManagerException;
import org.clever.canal.parse.exception.CanalParseException;
import org.clever.canal.protocol.position.LogPosition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 管理binlog消费位置信息(本地文件实现)
 *
 * <pre>
 * 策略：
 * 1. 先写内存，然后定时刷新数据到File
 * 2. 数据采取overwrite模式(只保留最后一次)
 * </pre>
 */
@SuppressWarnings({"DuplicatedCode", "unused"})
public class FileMixedLogPositionManager extends AbstractLogPositionManager {
    private final static Logger logger = LoggerFactory.getLogger(FileMixedLogPositionManager.class);

    /**
     * 文件读写使用的编码
     */
    private final static Charset CHARSET = StandardCharsets.UTF_8;
    /**
     * 空位置数据
     */
    private final static LogPosition Null_Position = new LogPosition() {
    };
    /**
     * 文件名称 "parse.dat"
     */
    private final static String Default_File_Name = "parse.dat.json";

    /**
     * 管理binlog消费位置信息(基于内存的实现)
     */
    private MemoryLogPositionManager memoryLogPositionManager;
    /**
     * 保存文件位置
     */
    private File dataDir;
    /**
     * 数据从内存写入硬盘时间间隔(单位ms)
     */
    private long period;
    /**
     * 管理Parse文件， 通道名称(destination) ---> Parse文件
     */
    private Map<String, File> dataFileCaches;
    /**
     * 线程池调度器
     */
    private ScheduledExecutorService executorService;
    /**
     * 需要保存数据的通道名称集合 --> Set<destination>
     */
    private Set<String> persistTasks;

    /**
     * @param dataDir                  保存文件位置
     * @param period                   数据从内存写入硬盘时间间隔(单位ms)
     * @param memoryLogPositionManager 管理binlog消费位置信息(基于内存的实现)
     */
    public FileMixedLogPositionManager(File dataDir, long period, MemoryLogPositionManager memoryLogPositionManager) {
        if (dataDir == null) {
            throw new NullPointerException("null dataDir");
        }
        if (period <= 0) {
            throw new IllegalArgumentException("period must be positive, given: " + period);
        }
        if (memoryLogPositionManager == null) {
            throw new NullPointerException("null memoryLogPositionManager");
        }
        this.dataDir = dataDir;
        this.period = period;
        this.memoryLogPositionManager = memoryLogPositionManager;
    }

    @Override
    public void start() {
        super.start();
        if (!dataDir.exists()) {
            try {
                FileUtils.forceMkdir(dataDir);
            } catch (IOException e) {
                throw new CanalMetaManagerException(e);
            }
        }
        if (!dataDir.canRead() || !dataDir.canWrite()) {
            throw new CanalMetaManagerException("dir[" + dataDir.getPath() + "] can not read/write");
        }
        if (!memoryLogPositionManager.isStart()) {
            memoryLogPositionManager.start();
        }
        this.dataFileCaches = MigrateMap.makeComputingMap(this::getDataFile);
        this.executorService = Executors.newScheduledThreadPool(1);
        this.persistTasks = Collections.synchronizedSet(new HashSet<>());
        // 启动定时工作任务
        executorService.scheduleAtFixedRate(() -> {
                    List<String> tasks = new ArrayList<>(persistTasks);
                    for (String destination : tasks) {
                        try {
                            // 定时将内存中的最新值刷到file中，多次变更只刷一次
                            flushDataToFile(destination);
                            persistTasks.remove(destination);
                        } catch (Throwable e) {
                            logger.error("period update" + destination + " cursor failed!", e);
                        }
                    }
                },
                period,
                period,
                TimeUnit.MILLISECONDS
        );
    }

    @Override
    public void stop() {
        super.stop();
        flushDataToFile();
        executorService.shutdown();
        memoryLogPositionManager.stop();
    }

    @Override
    public LogPosition getLatestIndexBy(String destination) {
        LogPosition logPosition = memoryLogPositionManager.getLatestIndexBy(destination);
        if (logPosition != null) {
            return logPosition;
        }
        logPosition = loadDataFromFile(dataFileCaches.get(destination));
        if (logPosition == null) {
            return Null_Position;
        }
        return logPosition;
    }

    @Override
    public void persistLogPosition(String destination, LogPosition logPosition) throws CanalParseException {
        persistTasks.add(destination);
        memoryLogPositionManager.persistLogPosition(destination, logPosition);
    }

    // ============================ helper method ======================

    private File getDataFile(String destination) {
        File destinationMetaDir = new File(dataDir, destination);
        if (!destinationMetaDir.exists()) {
            try {
                FileUtils.forceMkdir(destinationMetaDir);
            } catch (IOException e) {
                throw new CanalMetaManagerException(e);
            }
        }
        String dataFileName = "parse.dat";
        return new File(destinationMetaDir, dataFileName);
    }

    private void flushDataToFile() {
        for (String destination : memoryLogPositionManager.destinations()) {
            flushDataToFile(destination);
        }
    }

    private void flushDataToFile(String destination) {
        flushDataToFile(destination, dataFileCaches.get(destination));
    }

    private void flushDataToFile(String destination, File dataFile) {
        LogPosition position = memoryLogPositionManager.getLatestIndexBy(destination);
        if (position != null && position != Null_Position) {
            String json = JsonUtils.marshalToString(position);
            try {
                FileUtils.writeStringToFile(dataFile, json, CHARSET);
            } catch (IOException e) {
                throw new CanalMetaManagerException(e);
            }
        }
    }

    private LogPosition loadDataFromFile(File dataFile) {
        try {
            if (!dataFile.exists()) {
                return null;
            }
            String json = FileUtils.readFileToString(dataFile, CHARSET.name());
            return JsonUtils.unmarshalFromString(json, LogPosition.class);
        } catch (IOException e) {
            throw new CanalMetaManagerException(e);
        }
    }
}
