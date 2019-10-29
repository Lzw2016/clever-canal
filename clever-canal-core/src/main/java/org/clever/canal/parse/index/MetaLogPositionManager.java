package org.clever.canal.parse.index;

import org.clever.canal.common.utils.CollectionUtils;
import org.clever.canal.meta.CanalMetaManager;
import org.clever.canal.parse.exception.CanalParseException;
import org.clever.canal.protocol.ClientIdentity;
import org.clever.canal.protocol.position.LogPosition;
import org.clever.canal.store.helper.CanalEventUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

@SuppressWarnings("unused")
public class MetaLogPositionManager extends AbstractLogPositionManager {

    private final static Logger logger = LoggerFactory.getLogger(MetaLogPositionManager.class);

    private final CanalMetaManager metaManager;

    public MetaLogPositionManager(CanalMetaManager metaManager) {
        if (metaManager == null) {
            throw new NullPointerException("null metaManager");
        }

        this.metaManager = metaManager;
    }

    @Override
    public void stop() {
        super.stop();
        if (metaManager.isStart()) {
            metaManager.stop();
        }
    }

    @Override
    public void start() {
        super.start();
        if (!metaManager.isStart()) {
            metaManager.start();
        }
    }

    @Override
    public LogPosition getLatestIndexBy(String destination) {
        List<ClientIdentity> clientIdentities = metaManager.listAllSubscribeInfo(destination);
        LogPosition result = null;
        if (!CollectionUtils.isEmpty(clientIdentities)) {
            // 尝试找到一个最小的logPosition
            for (ClientIdentity clientIdentity : clientIdentities) {
                LogPosition position = (LogPosition) metaManager.getCursor(clientIdentity);
                if (position == null) {
                    continue;
                }
                if (result == null) {
                    result = position;
                } else {
                    result = CanalEventUtils.min(result, position);
                }
            }
        }
        return result;
    }

    @Override
    public void persistLogPosition(String destination, LogPosition logPosition) throws CanalParseException {
        // do nothing
        logger.info("persist LogPosition:{} - {}", destination, logPosition);
    }
}
