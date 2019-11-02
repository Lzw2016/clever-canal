-- create database if not exists `clever-canal` default character set = utf8;
-- use `clever-canal`;

/* ====================================================================================================================
    meta_history -- 表结构变化明细表
==================================================================================================================== */
CREATE TABLE meta_history (
  id bigint GENERATED ALWAYS AS IDENTITY NOT NULL,
  gmt_create timestamp NOT NULL,
  gmt_modified timestamp NOT NULL,
  destination varchar(128) DEFAULT NULL,
  binlog_file varchar(64) DEFAULT NULL,
  binlog_offset bigint DEFAULT NULL,
  binlog_master_id varchar(64) DEFAULT NULL,
  binlog_timestamp bigint DEFAULT NULL,
  use_schema varchar(1024) DEFAULT NULL,
  sql_schema varchar(1024) DEFAULT NULL,
  sql_table varchar(1024) DEFAULT NULL,
  sql_text clob(16 M) DEFAULT NULL,
  sql_type varchar(1024) DEFAULT NULL,
  extra varchar(512) DEFAULT NULL,
  PRIMARY KEY (id),
  CONSTRAINT meta_history_binlog_file_offset UNIQUE (destination,binlog_master_id,binlog_file,binlog_offset)
);

create index meta_history_destination on meta_history(destination);
create index meta_history_destination_timestamp on meta_history(destination,binlog_timestamp);
create index meta_history_gmt_modified on meta_history(gmt_modified);
/*------------------------------------------------------------------------------------------------------------------------

--------------------------------------------------------------------------------------------------------------------------*/


/* ====================================================================================================================
    meta_snapshot -- 表结构记录表快照表
==================================================================================================================== */
CREATE TABLE meta_snapshot (
  id bigint GENERATED ALWAYS AS IDENTITY NOT NULL,
  gmt_create timestamp NOT NULL,
  gmt_modified timestamp NOT NULL,
  destination varchar(128) DEFAULT NULL,
  binlog_file varchar(64) DEFAULT NULL,
  binlog_offset bigint DEFAULT NULL,
  binlog_master_id varchar(64) DEFAULT NULL,
  binlog_timestamp bigint DEFAULT NULL,
  data clob(16 M) DEFAULT NULL,
  extra varchar(512) DEFAULT NULL,
  PRIMARY KEY (id),
  CONSTRAINT meta_snapshot_binlog_file_offset UNIQUE (destination,binlog_master_id,binlog_file,binlog_offset)
);

create index meta_snapshot_destination on meta_snapshot(destination);
create index meta_snapshot_destination_timestamp on meta_snapshot(destination,binlog_timestamp);
create index meta_snapshot_gmt_modified on meta_snapshot(gmt_modified);
/*------------------------------------------------------------------------------------------------------------------------

--------------------------------------------------------------------------------------------------------------------------*/

