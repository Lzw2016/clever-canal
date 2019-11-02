package org.clever.canal.sink;

import lombok.Getter;
import lombok.Setter;
import org.clever.canal.common.AbstractCanalLifeCycle;
import org.clever.canal.filter.CanalEventFilter;
import org.clever.canal.store.model.Event;

import java.util.ArrayList;
import java.util.List;

@SuppressWarnings({"WeakerAccess", "unused"})
public abstract class AbstractCanalEventSink<T> extends AbstractCanalLifeCycle implements CanalEventSink<T> {
    @Getter
    @Setter
    protected CanalEventFilter<String> filter;
    @Getter
    protected List<CanalEventDownStreamHandler<List<Event>>> handlers = new ArrayList<>();

    @Override
    public void interrupt() {
        // do nothing
    }

    public CanalEventDownStreamHandler getHandler(int index) {
        return this.handlers.get(index);
    }

    public void addHandler(CanalEventDownStreamHandler<List<Event>> handler) {
        this.handlers.add(handler);
    }

    public void addHandler(CanalEventDownStreamHandler<List<Event>> handler, int index) {
        this.handlers.add(index, handler);
    }

    public void removeHandler(int index) {
        this.handlers.remove(index);
    }

    public void removeHandler(CanalEventDownStreamHandler handler) {
        this.handlers.remove(handler);
    }
}
