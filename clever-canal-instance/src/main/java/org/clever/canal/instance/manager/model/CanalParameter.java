package org.clever.canal.instance.manager.model;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.clever.canal.common.CanalException;
import org.clever.canal.common.utils.CanalToStringStyle;
import org.clever.canal.store.model.BatchMode;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * canal运行相关参数
 */
@Setter
@Getter
public class CanalParameter implements Serializable {
    private static final long serialVersionUID = -5893459662315430900L;

    // ============================================================================================================================== CanalAlarmHandler
    /**
     * 告警模式
     */
    private AlarmMode alarmMode = AlarmMode.LOGGER;

    // ============================================================================================================================== CanalMetaManager
    /**
     * MetaManager 的存储模式
     */
    private MetaMode metaMode = MetaMode.MEMORY;
    /**
     * MetaMode.LOCAL_FILE时，文件存储路径
     */
    private String metaDataDir = "./meta-manager";
    /**
     * MetaMode.LOCAL_FILE时，Meta刷新到文件系统的时间间隔(单位：ms)
     */
    private int metaFileFlushPeriod = 1000;

    // ============================================================================================================================== CanalEventStore
    /**
     * EventStore存储机制
     */
    private StorageMode storageMode = StorageMode.MEMORY;
    /**
     * StorageMode.MEMORY时，内存存储的buffer大小
     */
    private int memoryStorageBufferSize = 16 * 1024;
    /**
     * StorageMode.MEMORY时，内存存储的buffer内存占用单位，默认为1kb
     */
    private int memoryStorageBufferMemUnit = 1024;
    /**
     * StorageMode.MEMORY时，EventStore Batch模式
     */
    private BatchMode storageBatchMode = BatchMode.MEM_SIZE;
    /**
     * StorageMode.MEMORY时，内存存储的对象是否启用raw的ByteString模式<br />
     * 使用server/client模式时, 建议开启raw模式(raw = true)<br />
     * 使用Embedded模式时, 建议关闭raw模式(raw = false)<br />
     */
    private boolean memoryStorageRawEntry = true;
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

    // ============================================================================================================================== CanalEventParser
    // ============================================================ AbstractEventParser
    /**
     * 支持处理的transaction事务大小
     */
    private int transactionSize = 1024;
    /**
     * 匹配黑名单,忽略解析
     */
    private String blackFilter = null;
    // ============================================================ 常规配置
    /**
     * 数据来源类型
     */
    private SourcingType sourcingType = SourcingType.MYSQL;
    /**
     * binlog 消费位置信息 (EntryPosition.class Json字符串)
     */
    private List<String> positions;
    /**
     * 数据库发生切换查找时回退的时间
     */
    private int fallbackIntervalInSeconds = 60;
    /**
     * 是否忽略表解析异常
     */
    private boolean filterTableError = false;
    /**
     * 是否开启并行解析模式
     */
    private boolean parallel = false;
    /**
     * 是否开启GtId
     */
    private boolean gtIdEnable = false;
    /**
     * 连接的编码信息
     */
    private byte connectionCharsetNumber = (byte) 33;
    /**
     * 连接的编码
     */
    private String connectionCharset = "UTF-8";
    /**
     * 发送缓冲区大小
     */
    private int sendBufferSize = 64 * 1024;
    /**
     * 接受缓冲区大小
     */
    private int receiveBufferSize = 64 * 1024;
    // ============================================================ Server Binlog
    /**
     * 默认的连接超时时间(单位：秒)
     */
    private int defaultConnectionTimeoutInSeconds = 30;
    /**
     * 链接到mysql的slaveId
     */
    private long slaveId;
    /**
     * 数据库链接信息，包含多组信息<br />
     * 每个集合中相同位置(index)的元素是一个 group
     */
    private List<List<DataSourcing>> groupDbAddresses;
    /**
     * 默认链接的数据库 schemaName
     */
    private String defaultDatabaseName;
    /**
     * SourcingType.RDS_MYSQL时，RDS Accesskey
     */
    private String rdsAccesskey;
    /**
     * SourcingType.RDS_MYSQL时，RDS SecretKey
     */
    private String rdsSecretKey;
    /**
     * SourcingType.RDS_MYSQL时，RDS Instance Id
     */
    private String rdsInstanceId;
    /**
     * SourcingType.MYSQL、SourcingType.LOCAL_BINLOG时，数据库用户
     */
    private String dbUsername;
    /**
     * SourcingType.MYSQL、SourcingType.LOCAL_BINLOG时，数据库密码
     */
    private String dbPassword;
    // ============================================================ Local Binlog
    /**
     * 本地localBinlog目录
     */
    private String localBinlogDirectory;
    // ============================================================ TsDb 配置
    /**
     * 是否开启 TableMetaTsDb
     */
    private boolean tsDbEnable = false;
    /**
     * 生成快照的时间间隔(单位：小时)
     */
    private int tsDbSnapshotInterval = 24;
    /**
     * 快照过期时间(单位：小时)
     */
    private int tsDbSnapshotExpire = 360;
    /**
     * TsDb 数据源驱动 ClassName
     */
    private String tsDbDriverClassName = "com.mysql.cj.jdbc.Driver";
    /**
     * TsDb 数据源 JdbcUrl
     */
    private String tsDbJdbcUrl;
    /**
     * TsDb 数据源 UserName
     */
    private String tsDbJdbcUserName;
    /**
     * TsDb 数据源 Password
     */
    private String tsDbJdbcPassword;
    // ============================================================ 心跳检查信息
    /**
     * 是否开启心跳检查
     */
    private boolean detectingEnable = true;
    /**
     * 心跳sql
     */
    private String detectingSQL = "SELECT 1";
    /**
     * 心跳检查检测频率(单位秒)
     */
    private int detectingIntervalInSeconds = 3;
    // ============================================================================================================================== CanalHAController
    /**
     * HA机制
     */
    private HAMode haMode = HAMode.HEARTBEAT;
    /**
     * 是否开启基于心跳检查的HA功能
     */
    private boolean heartbeatHaEnable = false;
    /**
     * 心跳检查重试次数
     */
    private int detectingRetryTimes = 3;

    // ============================================================================================================================== CanalLogPositionManager
    /**
     * CanalLogPositionManager 的存储模式
     */
    private LogPositionMode logPositionMode = LogPositionMode.MEMORY;

    /**
     * 新增一个数据源组(主库，备库)
     */
    @SuppressWarnings("WeakerAccess")
    public void addGroupDbAddresses(List<DataSourcing> list) {
        if (groupDbAddresses == null) {
            groupDbAddresses = new ArrayList<>(list.size());
            for (int i = 0; i < list.size(); i++) {
                groupDbAddresses.add(new ArrayList<>());
            }
        } else if (groupDbAddresses.size() != list.size()) {
            throw new CanalException("Group Size is " + groupDbAddresses.size() + ", list size is" + list.size() + ". not equals!");
        }
        int index = 0;
        for (List<DataSourcing> groupDbAddress : groupDbAddresses) {
            DataSourcing dataSourcing = list.get(index);
            groupDbAddress.add(dataSourcing);
            index++;
        }
    }

    public void addGroupDbAddresses(DataSourcing master, DataSourcing... standby) {
        List<DataSourcing> list = new ArrayList<>();
        list.add(master);
        if (standby != null && standby.length > 0) {
            list.addAll(Arrays.asList(standby));
        }
        addGroupDbAddresses(list);
    }

    public String toString() {
        return ToStringBuilder.reflectionToString(this, CanalToStringStyle.DEFAULT_STYLE);
    }
}
