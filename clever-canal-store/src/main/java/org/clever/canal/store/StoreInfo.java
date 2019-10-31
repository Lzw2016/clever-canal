package org.clever.canal.store;

@SuppressWarnings({"WeakerAccess", "unused"})
public class StoreInfo {

    private String storeName;
    private String filter;

    public String getStoreName() {
        return storeName;
    }

    public String getFilter() {
        return filter;
    }

    public void setStoreName(String storeName) {
        this.storeName = storeName;
    }

    public void setFilter(String filter) {
        this.filter = filter;
    }
}
