package org.clever.canal.parse.inbound;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.clever.canal.parse.dbsync.binlog.event.TableMapLogEvent;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 描述数据meta对象,mysql binlog中对应的{@linkplain TableMapLogEvent}包含的信息不全
 *
 * <pre>
 * 1. 主键信息
 * 2. column name
 * 3. unsigned字段
 * </pre>
 */
@Getter
@Setter
public class TableMeta implements Serializable {
    /**
     * 数据库 schema 名称
     */
    private String schema;
    /**
     * table 名称
     */
    private String table;
    /**
     * 表结构的DDL语句
     */
    private String ddl;
    /**
     * 字段信息
     */
    private List<FieldMeta> fields = new ArrayList<>();

    public TableMeta() {
    }

    public TableMeta(String schema, String table, List<FieldMeta> fields) {
        this.schema = schema;
        this.table = table;
        this.fields = fields;
    }

    /**
     * ${schema}.${table}
     */
    public String getFullName() {
        return schema + "." + table;
    }

    /**
     * 新增字段
     */
    public void addFieldMeta(FieldMeta fieldMeta) {
        this.fields.add(fieldMeta);
    }

    /**
     * 获取字段信息
     *
     * @param name 字段名
     */
    public FieldMeta getFieldMetaByName(String name) {
        for (FieldMeta meta : fields) {
            if (meta.getColumnName().equalsIgnoreCase(name)) {
                return meta;
            }
        }
        throw new RuntimeException("unknown column : " + name);
    }

    /**
     * 获取表的主键字段信息
     */
    public List<FieldMeta> getPrimaryFields() {
        List<FieldMeta> primaryList = new ArrayList<>();
        for (FieldMeta meta : fields) {
            if (meta.isKey()) {
                primaryList.add(meta);
            }
        }
        return primaryList;
    }

    @Override
    public String toString() {
        StringBuilder data = new StringBuilder();
        data.append("TableMeta [schema=").append(schema).append(", table=").append(table).append(", fields=");
        for (FieldMeta field : fields) {
            data.append("\n\t").append(field.toString());
        }
        data.append("\n]");
        return data.toString();
    }

    @Getter
    @Setter
    public static class FieldMeta implements Serializable {

        private String columnName;
        private String columnType;
        private boolean nullable;
        private boolean key;
        private String defaultValue;
        private String extra;
        private boolean unique;

        public FieldMeta() {
        }

        public FieldMeta(String columnName, String columnType, boolean nullable, boolean key, String defaultValue) {
            this.columnName = columnName;
            this.columnType = columnType;
            this.nullable = nullable;
            this.key = key;
            this.defaultValue = defaultValue;
        }

        public boolean isUnsigned() {
            return StringUtils.containsIgnoreCase(columnType, "unsigned");
        }

        @Override
        public String toString() {
            return "FieldMeta [columnName=" + columnName + ", columnType=" + columnType + ", nullable=" + nullable
                    + ", key=" + key + ", defaultValue=" + defaultValue + ", extra=" + extra + ", unique=" + unique
                    + "]";
        }
    }
}
