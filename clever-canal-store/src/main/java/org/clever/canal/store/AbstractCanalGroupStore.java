package org.clever.canal.store;

import org.clever.canal.common.AbstractCanalLifeCycle;
import org.clever.canal.common.utils.Assert;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@SuppressWarnings({"WeakerAccess", "unused"})
public abstract class AbstractCanalGroupStore<T> extends AbstractCanalLifeCycle implements CanalGroupEventStore<T> {

    protected Map<String, StoreInfo> stores = new ConcurrentHashMap<>();

    @Override
    public void addStoreInfo(StoreInfo info) {
        checkInfo(info);
        stores.put(info.getStoreName(), info);
    }

    protected void checkInfo(StoreInfo info) {
        Assert.notNull(info);
        Assert.hasText(info.getStoreName());
    }
}
