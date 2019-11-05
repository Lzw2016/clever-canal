package org.clever.canal.instance.manager.model;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.clever.canal.common.utils.CanalToStringStyle;

import java.io.Serializable;
import java.util.Date;

/**
 * 对应的canal模型对象
 */
@NoArgsConstructor
@Getter
@Setter
public class Canal implements Serializable {
    private static final long serialVersionUID = 8333284022624682754L;

    /**
     * canalId (唯一)
     */
    private Long id;
    /**
     * 通道名称
     */
    private String destination;
    /**
     * 描述(不重要)
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

    /**
     * @param id             canalId (唯一)
     * @param destination    通道名称
     * @param canalParameter 参数定义
     */
    public Canal(Long id, String destination, CanalParameter canalParameter) {
        this.id = id;
        this.destination = destination;
        this.canalParameter = canalParameter;
    }

    public String toString() {
        return ToStringBuilder.reflectionToString(this, CanalToStringStyle.DEFAULT_STYLE);
    }
}
