package org.clever.canal.meta;

import org.apache.commons.io.FileUtils;
import org.clever.canal.common.utils.Assert;
import org.clever.canal.common.utils.JsonUtils;
import org.clever.canal.common.utils.MigrateMap;
import org.clever.canal.meta.exception.CanalMetaManagerException;
import org.clever.canal.protocol.ClientIdentity;
import org.clever.canal.protocol.position.LogPosition;
import org.clever.canal.protocol.position.Position;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 基于文件刷新的metaManager实现
 *
 * <pre>
 * 策略：
 * 1. 先写内存，然后定时刷新数据到File
 * 2. 数据采取overwrite模式(只保留最后一次)，通过logger实施append模式(记录历史版本)
 * </pre>
 */
@SuppressWarnings({"FieldCanBeLocal", "DuplicatedCode", "unused", "WeakerAccess"})
public class FileMixedMetaManager extends MemoryMetaManager implements CanalMetaManager {
    private static final Logger logger = LoggerFactory.getLogger(FileMixedMetaManager.class);

    private static final Charset charset = StandardCharsets.UTF_8;
    private static final Position Null_Cursor = new Position() {
    };

    /**
     * 保存文件位置
     */
    private final File dataDir;
    /**
     * 文件名称 "meta.dat"
     */
    private final String fileName;
    /**
     * 数据从内存写入硬盘时间间隔(单位ms)
     */
    private final long period;
    /**
     * 管理Meta文件， 数据源名称(destination) ---> meta文件
     */
    private Map<String, File> dataFileCaches;
    /**
     * 线程池调度器
     */
    private ScheduledExecutorService executor;
    /**
     * 需要保存数据的客户端标识集合
     */
    private Set<ClientIdentity> updateCursorTasks;

    /**
     * @param dataDir  保存文件位置
     * @param fileName 文件名称
     * @param period   数据从内存写入硬盘时间间隔(单位ms)
     */
    public FileMixedMetaManager(File dataDir, String fileName, long period) {
        Assert.notNull(dataDir);
        Assert.hasText(fileName);
        this.dataDir = dataDir;
        this.fileName = fileName;
        this.period = period <= 0 ? 1000 : period;
    }

    /**
     * @param dataDir 保存文件位置
     */
    public FileMixedMetaManager(File dataDir) {
        this(dataDir, "meta.dat", 1000);
    }

    /**
     * 初始化 CanalMetaManager
     */
    @Override
    public void start() {
        super.start();
        Assert.notNull(dataDir);
        // 文件夹不存在创建文件
        if (!dataDir.exists()) {
            try {
                FileUtils.forceMkdir(dataDir);
            } catch (IOException e) {
                throw new CanalMetaManagerException(e);
            }
        }
        // 保证文件夹能够读写
        if (!dataDir.canRead() || !dataDir.canWrite()) {
            throw new CanalMetaManagerException("dir[" + dataDir.getPath() + "] can not read/write");
        }
        dataFileCaches = MigrateMap.makeComputingMap(this::getDataFile);
        executor = Executors.newScheduledThreadPool(1);
        destinations = MigrateMap.makeComputingMap(this::loadClientIdentity);
        cursors = MigrateMap.makeComputingMap(clientIdentity -> {
            Assert.notNull(clientIdentity);
            Position position = loadCursor(clientIdentity.getDestination(), clientIdentity);
            if (position == null) {
                // 返回一个空对象标识，避免出现异常
                return Null_Cursor;
            } else {
                return position;
            }
        });
        updateCursorTasks = Collections.synchronizedSet(new HashSet<>());
        // 启动定时工作任务
        executor.scheduleAtFixedRate(
                () -> {
                    List<ClientIdentity> tasks = new ArrayList<>(updateCursorTasks);
                    for (ClientIdentity clientIdentity : tasks) {
                        MDC.put("destination", String.valueOf(clientIdentity.getDestination()));
                        try {
                            // 定时将内存中的最新值刷到file中，多次变更只刷一次
                            if (logger.isInfoEnabled()) {
                                LogPosition cursor = (LogPosition) getCursor(clientIdentity);
                                logger.info(
                                        "clientId:{} cursor:[{},{},{},{},{}] address[{}]",
                                        clientIdentity.getClientId(),
                                        cursor.getPosition().getJournalName(),
                                        cursor.getPosition().getPosition(),
                                        cursor.getPosition().getTimestamp(),
                                        cursor.getPosition().getServerId(),
                                        cursor.getPosition().getGtId(),
                                        cursor.getIdentity().getSourceAddress().toString()
                                );
                            }
                            // 持久化数据到文件系统
                            flushDataToFile(clientIdentity.getDestination());
                            // 移除保存成功的客户端标识
                            updateCursorTasks.remove(clientIdentity);
                        } catch (Throwable e) {
                            logger.error("period update" + clientIdentity.toString() + " curosr failed!", e);
                        } finally {
                            MDC.remove("destination");
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
        // 刷新数据到文件系统
        flushDataToFile();
        executor.shutdownNow();
        destinations.clear();
        batches.clear();
    }

    /**
     * 增加一个 client订阅 <br/>
     * 如果 client已经存在，则不做任何修改
     */
    @Override
    public void subscribe(final ClientIdentity clientIdentity) throws CanalMetaManagerException {
        super.subscribe(clientIdentity);
        // 订阅信息频率发生比较低，不需要做定时merge处理
        executor.submit(() -> flushDataToFile(clientIdentity.getDestination()));
    }

    /**
     * 取消client订阅
     */
    @Override
    public void unsubscribe(final ClientIdentity clientIdentity) throws CanalMetaManagerException {
        super.unsubscribe(clientIdentity);
        // 订阅信息频率发生比较低，不需要做定时merge处理
        executor.submit(() -> flushDataToFile(clientIdentity.getDestination()));
    }

    /**
     * 更新 cursor 游标
     */
    @Override
    public void updateCursor(ClientIdentity clientIdentity, Position position) throws CanalMetaManagerException {
        // 添加到任务队列中进行触发
        updateCursorTasks.add(clientIdentity);
        super.updateCursor(clientIdentity, position);
    }

    /**
     * 获取 cursor 游标
     */
    @Override
    public Position getCursor(ClientIdentity clientIdentity) throws CanalMetaManagerException {
        Position position = super.getCursor(clientIdentity);
        if (position == Null_Cursor) {
            return null;
        } else {
            return position;
        }
    }

    // ============================ helper method ======================

    /**
     * 根据数据源名称(destination)创建文件夹
     */
    private File getDataFile(String destination) {
        File destinationMetaDir = new File(dataDir, destination);
        if (!destinationMetaDir.exists()) {
            try {
                FileUtils.forceMkdir(destinationMetaDir);
            } catch (IOException e) {
                throw new CanalMetaManagerException(e);
            }
        }
        return new File(destinationMetaDir, fileName);
    }

    /**
     * 从文件加载Meta数据
     *
     * @param dataFile 文件
     */
    private FileMetaInstanceData loadDataFromFile(File dataFile) {
        try {
            if (!dataFile.exists()) {
                return null;
            }
            String json = FileUtils.readFileToString(dataFile, charset.name());
            return JsonUtils.unmarshalFromString(json, FileMetaInstanceData.class);
        } catch (IOException e) {
            throw new CanalMetaManagerException(e);
        }
    }

    /**
     * 把所有的数据源(destination)对应的Meta数据写入文件
     */
    private void flushDataToFile() {
        for (String destination : destinations.keySet()) {
            flushDataToFile(destination);
        }
    }

    /**
     * 把数据源(destination)对应的Meta数据写入文件
     *
     * @param destination 数据源名称
     */
    private void flushDataToFile(String destination) {
        flushDataToFile(destination, dataFileCaches.get(destination));
    }

    /**
     * 把数据源(destination)对应的Meta数据写入文件
     *
     * @param destination 数据源名称
     * @param dataFile    文件
     */
    private void flushDataToFile(String destination, File dataFile) {
        FileMetaInstanceData data = new FileMetaInstanceData();
        if (destinations.containsKey(destination)) {
            // 基于destination控制一下并发更新
            synchronized (destination.intern()) {
                data.setDestination(destination);
                List<FileMetaClientIdentityData> clientDataList = new ArrayList<>();
                List<ClientIdentity> clientIdentities = destinations.get(destination);
                for (ClientIdentity clientIdentity : clientIdentities) {
                    FileMetaClientIdentityData clientData = new FileMetaClientIdentityData();
                    clientData.setClientIdentity(clientIdentity);
                    Position position = cursors.get(clientIdentity);
                    if (position != null && position != Null_Cursor) {
                        clientData.setCursor((LogPosition) position);
                    }
                    clientDataList.add(clientData);
                }
                data.setClientDataList(clientDataList);
            }
            String json = JsonUtils.marshalToString(data);
            try {
                FileUtils.writeStringToFile(dataFile, json, charset);
            } catch (IOException e) {
                throw new CanalMetaManagerException(e);
            }
        }
    }

    /**
     * 从本地文件加载Meta数据，返回所有的客户端标识
     *
     * @param destination 数据源名称
     */
    private List<ClientIdentity> loadClientIdentity(String destination) {
        List<ClientIdentity> result = new ArrayList<>();
        FileMetaInstanceData data = loadDataFromFile(dataFileCaches.get(destination));
        if (data == null) {
            return result;
        }
        List<FileMetaClientIdentityData> clientDataList = data.getClientDataList();
        if (clientDataList == null) {
            return result;
        }
        for (FileMetaClientIdentityData clientData : clientDataList) {
            if (clientData.getClientIdentity().getDestination().equals(destination)) {
                result.add(clientData.getClientIdentity());
            }
        }
        return result;
    }

    /**
     * 从本地文件加载Meta数据，返回客户端当前位置信息(Position)
     *
     * @param destination    数据源名称
     * @param clientIdentity 客户端标识
     */
    private Position loadCursor(String destination, ClientIdentity clientIdentity) {
        FileMetaInstanceData data = loadDataFromFile(dataFileCaches.get(destination));
        if (data == null) {
            return null;
        }
        List<FileMetaClientIdentityData> clientDataList = data.getClientDataList();
        if (clientDataList == null) {
            return null;
        }
        for (FileMetaClientIdentityData clientData : clientDataList) {
            if (clientData.getClientIdentity() != null && clientData.getClientIdentity().equals(clientIdentity)) {
                return clientData.getCursor();
            }
        }
        return null;
    }

    // ============================

    /**
     * 描述一个clientIdentity对应的数据对象
     */
    @SuppressWarnings("WeakerAccess")
    public static class FileMetaClientIdentityData implements Serializable {
        /**
         * 客户端标识
         */
        private ClientIdentity clientIdentity;
        /**
         * binlog位置标识
         */
        private LogPosition cursor;

        public FileMetaClientIdentityData() {
        }

        public FileMetaClientIdentityData(ClientIdentity clientIdentity, MemoryClientIdentityBatch batch, LogPosition cursor) {
            this.clientIdentity = clientIdentity;
            this.cursor = cursor;
        }

        public ClientIdentity getClientIdentity() {
            return clientIdentity;
        }

        public void setClientIdentity(ClientIdentity clientIdentity) {
            this.clientIdentity = clientIdentity;
        }

        public Position getCursor() {
            return cursor;
        }

        public void setCursor(LogPosition cursor) {
            this.cursor = cursor;
        }
    }

    // ============================

    /**
     * 描述整个canal instance对应数据对象
     */
    @SuppressWarnings({"WeakerAccess"})
    public static class FileMetaInstanceData implements Serializable {
        /**
         * 数据源名称(destination)
         */
        private String destination;
        /**
         * 客户端Meta数据集合
         */
        private List<FileMetaClientIdentityData> clientDataList;

        public FileMetaInstanceData() {
        }

        public FileMetaInstanceData(String destination, List<FileMetaClientIdentityData> clientDataList) {
            this.destination = destination;
            this.clientDataList = clientDataList;
        }

        public String getDestination() {
            return destination;
        }

        public void setDestination(String destination) {
            this.destination = destination;
        }

        public List<FileMetaClientIdentityData> getClientDataList() {
            return clientDataList;
        }

        public void setClientDataList(List<FileMetaClientIdentityData> clientDataList) {
            this.clientDataList = clientDataList;
        }
    }
}
