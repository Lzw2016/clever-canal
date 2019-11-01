package org.clever.canal.store.model;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.clever.canal.common.utils.CanalToStringStyle;
import org.clever.canal.protocol.position.PositionRange;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 代表一组数据对象的集合
 */
@Getter
@Setter
public class Events<EVENT> implements Serializable {
    private static final long serialVersionUID = -7337454954300706044L;

    private PositionRange positionRange = new PositionRange();
    private List<EVENT> events = new ArrayList<>();

    public String toString() {
        return ToStringBuilder.reflectionToString(this, CanalToStringStyle.DEFAULT_STYLE);
    }
}
