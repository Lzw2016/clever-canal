-- create database if not exists `clever-canal` default character set = utf8;
-- use `clever-canal`;

/* ====================================================================================================================
    meta_history -- 表结构变化明细表
==================================================================================================================== */
create table meta_history (
  id bigint generated always as identity not null,
  gmt_create timestamp not null,
  gmt_modified timestamp not null,
  destination varchar(128) default null,
  binlog_file varchar(64) default null,
  binlog_offset bigint default null,
  binlog_master_id varchar(64) default null,
  binlog_timestamp bigint default null,
  use_schema varchar(1024) default null,
  sql_schema varchar(1024) default null,
  sql_table varchar(1024) default null,
  sql_text clob(16 m) default null,
  sql_type varchar(1024) default null,
  extra varchar(512) default null,
  primary key (id),
  constraint meta_history_binlog_file_offset unique (destination,binlog_master_id,binlog_file,binlog_offset)
);

create index meta_history_destination on meta_history(destination);
create index meta_history_destination_timestamp on meta_history(destination,binlog_timestamp);
create index meta_history_gmt_modified on meta_history(gmt_modified);
/*------------------------------------------------------------------------------------------------------------------------

--------------------------------------------------------------------------------------------------------------------------*/


/* ====================================================================================================================
    meta_snapshot -- 表结构记录表快照表
==================================================================================================================== */
create table meta_snapshot (
  id bigint generated always as identity not null,
  gmt_create timestamp not null,
  gmt_modified timestamp not null,
  destination varchar(128) default null,
  binlog_file varchar(64) default null,
  binlog_offset bigint default null,
  binlog_master_id varchar(64) default null,
  binlog_timestamp bigint default null,
  data clob(16 m) default null,
  extra varchar(512) default null,
  primary key (id),
  constraint meta_snapshot_binlog_file_offset unique (destination,binlog_master_id,binlog_file,binlog_offset)
);

create index meta_snapshot_destination on meta_snapshot(destination);
create index meta_snapshot_destination_timestamp on meta_snapshot(destination,binlog_timestamp);
create index meta_snapshot_gmt_modified on meta_snapshot(gmt_modified);
/*------------------------------------------------------------------------------------------------------------------------

--------------------------------------------------------------------------------------------------------------------------*/

