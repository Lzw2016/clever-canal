package org.clever.canal.sink;

import org.clever.canal.common.AbstractCanalLifeCycle;
import org.clever.canal.filter.CanalEventFilter;

import java.util.ArrayList;
import java.util.List;

@SuppressWarnings({"WeakerAccess", "unused"})
public abstract class AbstractCanalEventSink<T> extends AbstractCanalLifeCycle implements CanalEventSink<T> {

    protected CanalEventFilter filter;
    protected List<CanalEventDownStreamHandler> handlers = new ArrayList<>();

    public void setFilter(CanalEventFilter filter) {
        this.filter = filter;
    }

    public void addHandler(CanalEventDownStreamHandler handler) {
        this.handlers.add(handler);
    }

    public CanalEventDownStreamHandler getHandler(int index) {
        return this.handlers.get(index);
    }

    public void addHandler(CanalEventDownStreamHandler handler, int index) {
        this.handlers.add(index, handler);
    }

    public void removeHandler(int index) {
        this.handlers.remove(index);
    }

    public void removeHandler(CanalEventDownStreamHandler handler) {
        this.handlers.remove(handler);
    }

    public CanalEventFilter getFilter() {
        return filter;
    }

    public List<CanalEventDownStreamHandler> getHandlers() {
        return handlers;
    }

    public void interrupt() {
        // do nothing
    }
}
