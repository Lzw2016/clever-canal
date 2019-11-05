package org.clever.canal.instance.core;

import lombok.Data;

@Data
public class CanalMQConfig {

    private String topic;
    private Integer partition;
    private Integer partitionsNum;
    private String partitionHash;
    private String dynamicTopic;
}
