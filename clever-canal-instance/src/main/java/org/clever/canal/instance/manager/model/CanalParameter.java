package org.clever.canal.instance.manager.model;

import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.clever.canal.common.utils.CanalToStringStyle;
import org.clever.canal.store.model.BatchMode;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * canal运行相关参数
 */
public class CanalParameter implements Serializable {
    private static final long serialVersionUID = -5893459662315430900L;

    // ============================================================================================================================== 报警机制参数
    /**
     * 告警模式
     */
    @Setter
    @Getter
    private AlarmMode alarmMode = AlarmMode.LOGGER;

    // ============================================================================================================================== meta相关参数
    /**
     * MetaManager 的存储模式
     */
    private MetaMode metaMode = MetaMode.MEMORY;
    /**
     * MetaMode.LOCAL_FILE时，文件存储路径
     */
    @Setter
    @Getter
    private String metaDataDir = "./meta-manager";
    /**
     * MetaMode.LOCAL_FILE时，Meta刷新到文件系统的时间间隔(单位：ms)
     */
    private Integer metaFileFlushPeriod = 1000;

    // ============================================================================================================================== storage存储
    /**
     * EventStore存储机制
     */
    private StorageMode storageMode = StorageMode.MEMORY;
    /**
     * StorageMode.MEMORY时，内存存储的buffer大小
     */
    private Integer memoryStorageBufferSize = 16 * 1024;
    /**
     * StorageMode.MEMORY时，内存存储的buffer内存占用单位，默认为1kb
     */
    private Integer memoryStorageBufferMemUnit = 1024;
    /**
     * StorageMode.MEMORY时，EventStore Batch模式
     */
    private BatchMode storageBatchMode = BatchMode.MEM_SIZE;
    /**
     * StorageMode.MEMORY时，内存存储的对象是否启用raw的ByteString模式<br />
     * 使用server/client模式时, 建议开启raw模式(raw = true)<br />
     * 使用Embedded模式时, 建议关闭raw模式(raw = false)<br />
     */
    private Boolean memoryStorageRawEntry = Boolean.TRUE;
    /**
     * 是否将ddl单条返回
     */
    private boolean ddlIsolation = false;
    /**
     * EventStore 内存回收模式
     */
    private StorageScavengeMode storageScavengeMode = StorageScavengeMode.ON_ACK;
    /**
     * StorageScavengeMode.ON_SCHEDULE时，调度规则
     */
    private String scavengeSchedule;

    // ============================================================================================================================== replication相关参数
    /**
     * 数据来源类型
     */
    private SourcingType sourcingType = SourcingType.MYSQL;
    /**
     * 连接的编码信息
     */
    private Byte connectionCharsetNumber = (byte) 33;
    /**
     * 连接的编码
     */
    private String connectionCharset = "UTF-8";
    /**
     * 默认的连接超时时间(单位：秒)
     */
    private Integer defaultConnectionTimeoutInSeconds = 30;
    /**
     * 发送缓冲区大小
     */
    private Integer sendBufferSize = 64 * 1024;
    /**
     * 接受缓冲区大小
     */
    private Integer receiveBufferSize = 64 * 1024;

    // ============================================================================================================================== 数据库信息
    /**
     * 数据库链接信息
     */
    private List<InetSocketAddress> dbAddresses;
    /**
     * 数据库链接信息，包含多组信息
     */
    private List<List<DataSourcing>> groupDbAddresses;
    /**
     * 数据库用户
     */
    private String dbUsername;
    /**
     * 数据库密码
     */
    private String dbPassword;
    /**
     * RDS Accesskey
     */
    private String rdsAccesskey;
    /**
     * RDS SecretKey
     */
    private String rdsSecretKey;
    /**
     * RDS Instance Id
     */
    private String rdsInstanceId;

    // ============================================================================================================================== 心跳检查信息
    /**
     * 是否开启心跳检查
     */
    private Boolean detectingEnable = true;
    /**
     * 心跳sql
     */
    private String detectingSQL;
    /**
     * 心跳检查检测频率(单位秒)
     */
    private Integer detectingIntervalInSeconds = 3;
    /**
     * 链接到mysql的slaveId
     */
    private Long slaveId;


    // ============================================================================================================================== CanalEventParser
    /**
     * 默认链接的数据库 schemaName
     */
    private String defaultDatabaseName;
    private List<String> positions;                                                 // 数据库positions信息
    private String masterLogfileName = null;                                        // master起始位置
    private Long masterLogfileOffest = null;
    private Long masterTimestamp = null;
    private String standbyLogfileName = null;                                       // standby起始位置
    private Long standbyLogfileOffest = null;
    private Long standbyTimestamp = null;
    /**
     * 数据库发生切换查找时回退的时间
     */
    private Integer fallbackIntervalInSeconds = 60;
    /**
     * 是否忽略表解析异常
     */
    private Boolean filterTableError = Boolean.FALSE;                               //
    private Boolean parallel = Boolean.FALSE;
    /**
     * 是否开启GtId
     */
    private Boolean gtIdEnable = Boolean.FALSE;                                     //

    private Integer tsdbSnapshotInterval = 24;
    private Integer tsdbSnapshotExpire = 360;
    /**
     * 是否开启 TableMetaTsDb
     */
    private Boolean tsdbEnable = Boolean.FALSE;                                     //
    @Setter
    @Getter
    private String tsDbDriverClassName = "com.mysql.cj.jdbc.Driver";
    private String tsdbJdbcUrl;
    private String tsdbJdbcUserName;
    private String tsdbJdbcPassword;

    /**
     * 本地localBinlog目录
     */
    private String localBinlogDirectory;
    private Integer transactionSize = 1024;                                         // 支持处理的transaction事务大小

    private String blackFilter = null;                                              // 匹配黑名单,忽略解析


    // ============================================================================================================================== CanalHAController
    /**
     * 是否开启基于心跳检查的HA功能
     */
    private Boolean heartbeatHaEnable = false;
    /**
     * HA机制
     */
    private HAMode haMode = HAMode.HEARTBEAT;
    /**
     * 心跳检查重试次数
     */
    private Integer detectingRetryTimes = 3;


    // ============================================================================================================================== CanalLogPositionManager
    private LogPositionMode logPositionMode = LogPositionMode.MEMORY;

    // ==============================================================================================================================


    /**
     * 告警模式
     */
    public enum AlarmMode {
        /**
         * 写文件日志
         */
        LOGGER,
    }

    /**
     * MetaManager 的存储模式
     */
    public enum MetaMode {
        /**
         * 内存存储模式
         */
        MEMORY,
        /**
         * 本地文件存储模式(内存 + 本地文件存储)
         */
        LOCAL_FILE,
//        /**
//         * 文件存储模式
//         */
//        ZOOKEEPER,
//        /**
//         * 混合模式，内存+文件
//         */
//        MIXED;
    }

    /**
     * EventStore 存储模式
     */
    public enum StorageMode {
        /**
         * 内存存储模式
         */
        MEMORY,
//        /**
//         * 文件存储模式
//         */
//        FILE,
//        /**
//         * 混合模式，内存+文件
//         */
//        MIXED,
    }

    /**
     * EventStore 内存回收模式
     */
    @SuppressWarnings("unused")
    public enum StorageScavengeMode {
        /**
         * 在存储满的时候触发
         */
        ON_FULL,
        /**
         * 在每次有ack请求时触发
         */
        ON_ACK,
        /**
         * 定时触发，需要外部控制
         */
        ON_SCHEDULE,
        /**
         * 不做任何操作，由外部进行清理
         */
        NO_OP,
    }

    /**
     * 数据源类型
     */
    @SuppressWarnings("unused")
    public enum SourcingType {
        /**
         * mysql DB
         */
        MYSQL,
        /**
         * localBinLog
         */
        LOCAL_BINLOG,
        /**
         * oracle DB
         */
        ORACLE,
    }

    /**
     * 高可用模式
     */
    @SuppressWarnings("unused")
    public enum HAMode {
        /**
         * 心跳检测
         */
        HEARTBEAT,
        /**
         * otter media
         */
        MEDIA,
    }

    /**
     * CanalLogPositionManager 的存储模式
     */
    public enum LogPositionMode {
        /**
         * 内存存储模式
         */
        MEMORY,
        /**
         * 基于meta信息
         */
        META,
        /**
         * 基于内存+meta的failBack实现
         */
        MEMORY_META_FAIL_BACK,
//        /**
//         * 文件存储模式 zookeeper
//         */
//        ZOOKEEPER,
//        /**
//         * 混合模式，内存+文件
//         */
//        MIXED,
    }

//    /**
//     * 集群模式
//     */
//    public enum ClusterMode {
//        /**
//         * 嵌入式
//         */
//        STANDALONE,
//        /**
//         * 冷备
//         */
//        STANDBY,
//        /**
//         * 热备
//         */
//        ACTIVE,
//    }

    /**
     * 数据来源描述
     */
    @NoArgsConstructor
    @Data
    public static class DataSourcing implements Serializable {
        private static final long serialVersionUID = -1770648468678085234L;
        /**
         * 数据源类型
         */
        private SourcingType type;
        /**
         * 数据源地址
         */
        private InetSocketAddress dbAddress;

        @SuppressWarnings("WeakerAccess")
        public DataSourcing(SourcingType type, InetSocketAddress dbAddress) {
            this.type = type;
            this.dbAddress = dbAddress;
        }
    }


    public MetaMode getMetaMode() {
        return metaMode;
    }


    public StorageMode getStorageMode() {
        return storageMode;
    }


    public Integer getMetaFileFlushPeriod() {
        return metaFileFlushPeriod;
    }


    public Integer getMemoryStorageBufferSize() {
        return memoryStorageBufferSize;
    }


    public SourcingType getSourcingType() {
        return sourcingType;
    }


    public String getLocalBinlogDirectory() {
        return localBinlogDirectory;
    }


    public HAMode getHaMode() {
        return haMode;
    }


    public Integer getDefaultConnectionTimeoutInSeconds() {
        return defaultConnectionTimeoutInSeconds;
    }


    public Integer getReceiveBufferSize() {
        return receiveBufferSize;
    }


    public Integer getSendBufferSize() {
        return sendBufferSize;
    }


    public Byte getConnectionCharsetNumber() {
        return connectionCharsetNumber;
    }


    public String getConnectionCharset() {
        return connectionCharset;
    }

    public LogPositionMode getLogPositionMode() {
        return logPositionMode;
    }


    public String getDefaultDatabaseName() {
        return defaultDatabaseName;
    }


    public Long getSlaveId() {
        return slaveId;
    }

    public void setSlaveId(Long slaveId) {
        this.slaveId = slaveId;
    }

    public Boolean getDetectingEnable() {
        return detectingEnable;
    }


    public String getDetectingSQL() {
        return detectingSQL;
    }


    public Integer getDetectingIntervalInSeconds() {
        return detectingIntervalInSeconds;
    }


    public Integer getDetectingRetryTimes() {
        return detectingRetryTimes;
    }


    public StorageScavengeMode getStorageScavengeMode() {
        return storageScavengeMode;
    }

    public String getScavengeSchedule() {
        return scavengeSchedule;
    }


    public Integer getTransactionSize() {
        return transactionSize != null ? transactionSize : 1024;
    }


    public List<InetSocketAddress> getDbAddresses() {
        return dbAddresses;
    }

    public List<List<DataSourcing>> getGroupDbAddresses() {
        if (groupDbAddresses == null) {
            groupDbAddresses = new ArrayList<>();
            if (dbAddresses != null) {
                for (InetSocketAddress address : dbAddresses) {
                    List<DataSourcing> groupAddresses = new ArrayList<>();
                    groupAddresses.add(new DataSourcing(sourcingType, address));
                    groupDbAddresses.add(groupAddresses);
                }
            }
        }
        return groupDbAddresses;
    }


    public void setDbAddresses(List<InetSocketAddress> dbAddresses) {
        this.dbAddresses = dbAddresses;
    }

    public String getDbUsername() {
        return dbUsername;
    }

    public void setDbUsername(String dbUsername) {
        this.dbUsername = dbUsername;
    }

    public String getDbPassword() {
        return dbPassword;
    }

    public void setDbPassword(String dbPassword) {
        this.dbPassword = dbPassword;
    }

    public List<String> getPositions() {
        if (positions == null) {
            positions = new ArrayList<>();
            String masterPosition = buildPosition(masterLogfileName, masterLogfileOffest, masterTimestamp);
            if (masterPosition != null) {
                positions.add(masterPosition);
            }
            String standbyPosition = buildPosition(standbyLogfileName, standbyLogfileOffest, standbyTimestamp);
            if (standbyPosition != null) {
                positions.add(standbyPosition);
            }
        }
        return positions;
    }


    // ===========================兼容字段

    private String buildPosition(String journalName, Long position, Long timestamp) {
        StringBuilder masterBuilder = new StringBuilder();
        if (StringUtils.isNotEmpty(journalName) || position != null || timestamp != null) {
            masterBuilder.append('{');
            if (StringUtils.isNotEmpty(journalName)) {
                masterBuilder.append("\"journalName\":\"").append(journalName).append("\"");
            }

            if (position != null) {
                if (masterBuilder.length() > 1) {
                    masterBuilder.append(",");
                }
                masterBuilder.append("\"position\":").append(position);
            }

            if (timestamp != null) {
                if (masterBuilder.length() > 1) {
                    masterBuilder.append(",");
                }
                masterBuilder.append("\"timestamp\":").append(timestamp);
            }
            masterBuilder.append('}');
            return masterBuilder.toString();
        } else {
            return null;
        }
    }


    public Integer getFallbackIntervalInSeconds() {
        return fallbackIntervalInSeconds == null ? 60 : fallbackIntervalInSeconds;
    }


    public Boolean getHeartbeatHaEnable() {
        return heartbeatHaEnable == null ? false : heartbeatHaEnable;
    }


    public BatchMode getStorageBatchMode() {
        return storageBatchMode == null ? BatchMode.MEM_SIZE : storageBatchMode;
    }

    public void setStorageBatchMode(BatchMode storageBatchMode) {
        this.storageBatchMode = storageBatchMode;
    }

    public Integer getMemoryStorageBufferMemUnit() {
        return memoryStorageBufferMemUnit == null ? 1024 : memoryStorageBufferMemUnit;
    }


    public Boolean getDdlIsolation() {
        return ddlIsolation;
    }


    public Boolean getFilterTableError() {
        return filterTableError == null ? false : filterTableError;
    }


    public String getBlackFilter() {
        return blackFilter;
    }


    public Boolean getTsdbEnable() {
        return tsdbEnable;
    }

    public void setTsdbEnable(Boolean tsdbEnable) {
        this.tsdbEnable = tsdbEnable;
    }

    public String getTsdbJdbcUrl() {
        return tsdbJdbcUrl;
    }

    public void setTsdbJdbcUrl(String tsdbJdbcUrl) {
        this.tsdbJdbcUrl = tsdbJdbcUrl;
    }

    public String getTsdbJdbcUserName() {
        return tsdbJdbcUserName;
    }

    public void setTsdbJdbcUserName(String tsdbJdbcUserName) {
        this.tsdbJdbcUserName = tsdbJdbcUserName;
    }

    public String getTsdbJdbcPassword() {
        return tsdbJdbcPassword;
    }

    public void setTsdbJdbcPassword(String tsdbJdbcPassword) {
        this.tsdbJdbcPassword = tsdbJdbcPassword;
    }

    public String getRdsAccesskey() {
        return rdsAccesskey;
    }


    public String getRdsSecretKey() {
        return rdsSecretKey;
    }


    public String getRdsInstanceId() {
        return rdsInstanceId;
    }


    public Boolean getGtIdEnable() {
        return gtIdEnable;
    }


    public Boolean getMemoryStorageRawEntry() {
        return memoryStorageRawEntry;
    }

    public void setMemoryStorageRawEntry(Boolean memoryStorageRawEntry) {
        this.memoryStorageRawEntry = memoryStorageRawEntry;
    }

    public Integer getTsdbSnapshotInterval() {
        return tsdbSnapshotInterval;
    }


    public Integer getTsdbSnapshotExpire() {
        return tsdbSnapshotExpire;
    }


    public Boolean getParallel() {
        return parallel;
    }


    public String toString() {
        return ToStringBuilder.reflectionToString(this, CanalToStringStyle.DEFAULT_STYLE);
    }
}
