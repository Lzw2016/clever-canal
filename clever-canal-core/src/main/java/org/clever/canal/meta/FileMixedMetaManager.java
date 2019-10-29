package org.clever.canal.meta;

import com.google.common.collect.Lists;
import com.google.common.collect.MigrateMap;
import org.apache.commons.io.FileUtils;
import org.clever.canal.common.utils.Assert;
import org.clever.canal.common.utils.JsonUtils;
import org.clever.canal.meta.exception.CanalMetaManagerException;
import org.clever.canal.protocol.ClientIdentity;
import org.clever.canal.protocol.position.LogPosition;
import org.clever.canal.protocol.position.Position;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.File;
import java.io.IOException;
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
@SuppressWarnings({"FieldCanBeLocal", "DuplicatedCode", "unused"})
public class FileMixedMetaManager extends MemoryMetaManager implements CanalMetaManager {

    private static final Logger logger = LoggerFactory.getLogger(FileMixedMetaManager.class);
    private static final Charset charset = StandardCharsets.UTF_8;
    private File dataDir;
    private String dataFileName = "meta.dat";
    private Map<String, File> dataFileCaches;
    private ScheduledExecutorService executor;
    private final Position nullCursor = new Position() {
    };
    private long period = 1000; // 单位ms
    private Set<ClientIdentity> updateCursorTasks;

    public void start() {
        super.start();
        Assert.notNull(dataDir);
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
        dataFileCaches = MigrateMap.makeComputingMap(this::getDataFile);
        executor = Executors.newScheduledThreadPool(1);
        destinations = MigrateMap.makeComputingMap(this::loadClientIdentity);
        cursors = MigrateMap.makeComputingMap(clientIdentity -> {
            Assert.notNull(clientIdentity);
            Position position = loadCursor(clientIdentity.getDestination(), clientIdentity);
            if (position == null) {
                return nullCursor; // 返回一个空对象标识，避免出现异常
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
                                        cursor.getPostion().getJournalName(),
                                        cursor.getPostion().getPosition(),
                                        cursor.getPostion().getTimestamp(),
                                        cursor.getPostion().getServerId(),
                                        cursor.getPostion().getGtid(),
                                        cursor.getIdentity().getSourceAddress().toString()
                                );
                            }
                            flushDataToFile(clientIdentity.getDestination());
                            updateCursorTasks.remove(clientIdentity);
                        } catch (Throwable e) {
                            // ignore
                            logger.error("period update" + clientIdentity.toString() + " curosr failed!", e);
                        }
                    }
                },
                period,
                period,
                TimeUnit.MILLISECONDS
        );
    }

    public void stop() {
        super.stop();
        flushDataToFile();// 刷新数据
        executor.shutdownNow();
        destinations.clear();
        batches.clear();
    }

    public void subscribe(final ClientIdentity clientIdentity) throws CanalMetaManagerException {
        super.subscribe(clientIdentity);
        // 订阅信息频率发生比较低，不需要做定时merge处理
        executor.submit(() -> flushDataToFile(clientIdentity.getDestination()));
    }

    public void unsubscribe(final ClientIdentity clientIdentity) throws CanalMetaManagerException {
        super.unsubscribe(clientIdentity);
        // 订阅信息频率发生比较低，不需要做定时merge处理
        executor.submit(() -> flushDataToFile(clientIdentity.getDestination()));
    }

    public void updateCursor(ClientIdentity clientIdentity, Position position) throws CanalMetaManagerException {
        updateCursorTasks.add(clientIdentity);// 添加到任务队列中进行触发
        super.updateCursor(clientIdentity, position);
    }

    public Position getCursor(ClientIdentity clientIdentity) throws CanalMetaManagerException {
        Position position = super.getCursor(clientIdentity);
        if (position == nullCursor) {
            return null;
        } else {
            return position;
        }
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
        return new File(destinationMetaDir, dataFileName);
    }

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

    private void flushDataToFile() {
        for (String destination : destinations.keySet()) {
            flushDataToFile(destination);
        }
    }

    private void flushDataToFile(String destination) {
        flushDataToFile(destination, dataFileCaches.get(destination));
    }

    private void flushDataToFile(String destination, File dataFile) {
        FileMetaInstanceData data = new FileMetaInstanceData();
        if (destinations.containsKey(destination)) {
            synchronized (destination.intern()) { // 基于destination控制一下并发更新
                data.setDestination(destination);
                List<FileMetaClientIdentityData> clientDatas = Lists.newArrayList();
                List<ClientIdentity> clientIdentitys = destinations.get(destination);
                for (ClientIdentity clientIdentity : clientIdentitys) {
                    FileMetaClientIdentityData clientData = new FileMetaClientIdentityData();
                    clientData.setClientIdentity(clientIdentity);
                    Position position = cursors.get(clientIdentity);
                    if (position != null && position != nullCursor) {
                        clientData.setCursor((LogPosition) position);
                    }
                    clientDatas.add(clientData);
                }
                data.setClientDatas(clientDatas);
            }

            String json = JsonUtils.marshalToString(data);
            try {
                FileUtils.writeStringToFile(dataFile, json, charset);
            } catch (IOException e) {
                throw new CanalMetaManagerException(e);
            }
        }
    }

    private List<ClientIdentity> loadClientIdentity(String destination) {
        List<ClientIdentity> result = Lists.newArrayList();

        FileMetaInstanceData data = loadDataFromFile(dataFileCaches.get(destination));
        if (data == null) {
            return result;
        }

        List<FileMetaClientIdentityData> clientDatas = data.getClientDatas();
        if (clientDatas == null) {
            return result;
        }

        for (FileMetaClientIdentityData clientData : clientDatas) {
            if (clientData.getClientIdentity().getDestination().equals(destination)) {
                result.add(clientData.getClientIdentity());
            }
        }

        return result;
    }

    private Position loadCursor(String destination, ClientIdentity clientIdentity) {
        FileMetaInstanceData data = loadDataFromFile(dataFileCaches.get(destination));
        if (data == null) {
            return null;
        }

        List<FileMetaClientIdentityData> clientDatas = data.getClientDatas();
        if (clientDatas == null) {
            return null;
        }

        for (FileMetaClientIdentityData clientData : clientDatas) {
            if (clientData.getClientIdentity() != null && clientData.getClientIdentity().equals(clientIdentity)) {
                return clientData.getCursor();
            }
        }

        return null;
    }

    /**
     * 描述一个clientIdentity对应的数据对象
     */
    @SuppressWarnings("WeakerAccess")
    public static class FileMetaClientIdentityData {

        private ClientIdentity clientIdentity;
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

    /**
     * 描述整个canal instance对应数据对象
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public static class FileMetaInstanceData {
        private String destination;
        private List<FileMetaClientIdentityData> clientDatas;

        public FileMetaInstanceData() {
        }

        public FileMetaInstanceData(String destination, List<FileMetaClientIdentityData> clientDatas) {
            this.destination = destination;
            this.clientDatas = clientDatas;
        }

        public String getDestination() {
            return destination;
        }

        public void setDestination(String destination) {
            this.destination = destination;
        }

        public List<FileMetaClientIdentityData> getClientDatas() {
            return clientDatas;
        }

        public void setClientDatas(List<FileMetaClientIdentityData> clientDatas) {
            this.clientDatas = clientDatas;
        }
    }

    public void setDataDir(String dataDir) {
        this.dataDir = new File(dataDir);
    }

    public void setDataDirByFile(File dataDir) {
        this.dataDir = dataDir;
    }

    public void setPeriod(long period) {
        this.period = period;
    }
}
