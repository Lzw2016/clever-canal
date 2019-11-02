package org.clever.canal.parse.index;

import org.clever.canal.common.AbstractCanalLifeCycle;

/**
 * 管理binlog消费位置信息，抽象类
 */
@SuppressWarnings("WeakerAccess")
public abstract class AbstractLogPositionManager extends AbstractCanalLifeCycle implements CanalLogPositionManager {
}
