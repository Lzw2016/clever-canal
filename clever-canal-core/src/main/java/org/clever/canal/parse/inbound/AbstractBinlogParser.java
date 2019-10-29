package org.clever.canal.parse.inbound;

import org.clever.canal.common.AbstractCanalLifeCycle;
import org.clever.canal.parse.exception.CanalParseException;
import org.clever.canal.protocol.CanalEntry.Entry;

@SuppressWarnings("unused")
public abstract class AbstractBinlogParser<T> extends AbstractCanalLifeCycle implements BinlogParser<T> {

    public void reset() {
    }

    public Entry parse(T event, TableMeta tableMeta) throws CanalParseException {
        return null;
    }

    public Entry parse(T event) throws CanalParseException {
        return null;
    }

    public void stop() {
        reset();
        super.stop();
    }
}
