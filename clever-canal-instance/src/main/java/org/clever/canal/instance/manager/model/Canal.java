package org.clever.canal.instance.manager.model;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.clever.canal.common.utils.CanalToStringStyle;

import java.io.Serializable;
import java.util.Date;

/**
 * 对应的canal模型对象
 */
@Getter
@Setter
public class Canal implements Serializable {
    private static final long serialVersionUID = 8333284022624682754L;

    /**
     * canalId (唯一)
     */
    private Long id;
    /**
     * 对应的名字
     */
    private String name;
    /**
     * 描述
     */
    private String desc;
    /**
     * 运行状态
     */
    private CanalStatus status;
    /**
     * 参数定义
     */
    private CanalParameter canalParameter;
    /**
     * 创建时间
     */
    private Date gmtCreate = new Date();
    /**
     * 修改时间
     */
    private Date gmtModified = new Date();

    public String toString() {
        return ToStringBuilder.reflectionToString(this, CanalToStringStyle.DEFAULT_STYLE);
    }
}
