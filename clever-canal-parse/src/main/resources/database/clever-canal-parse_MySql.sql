create database if not exists `clever-canal` default character set = utf8;
use `clever-canal`;

/* ====================================================================================================================
    meta_history -- 表结构变化明细表
==================================================================================================================== */
create table meta_history
(
    id                  bigint(20)      not null        auto_increment                          comment '主键',
    gmt_create          datetime(3)     not null                                                comment '创建时间',
    gmt_modified        datetime(3)     not null                                                comment '修改时间',
    destination         varchar(127)                                                            comment '通道名称',
    binlog_file         varchar(63)                                                             comment 'binlog文件名',
    binlog_offset       bigint(20)                                                              comment 'binlog偏移量',
    binlog_master_id    varchar(63)                                                             comment 'binlog节点id',
    binlog_timestamp    bigint(20)                                                              comment 'binlog应用的时间戳',
    use_schema          varchar(1023)                                                           comment '执行sql时对应的schema',
    sql_schema          varchar(1023)                                                           comment '对应的schema',
    sql_table           varchar(1023)                                                           comment '对应的table',
    sql_text            longtext                                                                comment '执行的sql',
    sql_type            varchar(255)                                                            comment 'sql类型',
    extra               text                                                                    comment '额外的扩展信息',
    primary key (id),
    unique key meta_history_binlog_file_offest(destination, binlog_master_id, binlog_file, binlog_offset),
    key meta_history_destination (destination),
    key meta_history_destination_timestamp (destination, binlog_timestamp),
    key meta_history_gmt_modified (gmt_modified)
) engine=innodb auto_increment=1 default charset=utf8 comment='表结构变化明细表';
/*------------------------------------------------------------------------------------------------------------------------

--------------------------------------------------------------------------------------------------------------------------*/


/* ====================================================================================================================
    meta_snapshot -- 表结构记录表快照表
==================================================================================================================== */
create table meta_snapshot
(
    id                  bigint(20)      not null        auto_increment                          comment '主键',
    gmt_create          datetime(3)     not null                                                comment '创建时间',
    gmt_modified        datetime(3)     not null                                                comment '修改时间',
    destination         varchar(127)                                                            comment '通道名称',
    binlog_file         varchar(63)                                                             comment 'binlog文件名',
    binlog_offset       bigint(20)                                                              comment 'binlog偏移量',
    binlog_master_id    varchar(63)                                                             comment 'binlog节点id',
    binlog_timestamp    bigint(20)                                                              comment 'binlog应用的时间戳',
    data                longtext                                                                comment '表结构数据',
    extra               text                                                                    comment '额外的扩展信息',
    primary key (id),
    unique key meta_snapshot_binlog_file_offest(destination, binlog_master_id, binlog_file, binlog_offset),
    key meta_snapshot_destination (destination),
    key meta_snapshot_destination_timestamp (destination, binlog_timestamp),
    key meta_snapshot_gmt_modified (gmt_modified)
) engine=innodb auto_increment=1 default charset=utf8 comment='表结构记录表快照表';
/*------------------------------------------------------------------------------------------------------------------------

--------------------------------------------------------------------------------------------------------------------------*/

