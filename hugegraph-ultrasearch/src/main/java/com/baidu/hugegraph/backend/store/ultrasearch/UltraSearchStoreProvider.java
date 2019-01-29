package com.baidu.hugegraph.backend.store.ultrasearch;

import java.util.Iterator;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.store.AbstractBackendStoreProvider;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.backend.store.ultrasearch.UltraSearchStore.UltraSearchGraphStore;
import com.baidu.hugegraph.backend.store.ultrasearch.UltraSearchStore.UltraSearchSchemaStore;
import com.baidu.hugegraph.util.Events;

public class UltraSearchStoreProvider extends AbstractBackendStoreProvider {

    protected String database() {
        return this.graph().toLowerCase();
    }

    @Override
    protected BackendStore newSchemaStore(String store) {
        return new UltraSearchSchemaStore(this, this.database(), store);
    }

    @Override
    protected BackendStore newGraphStore(String store) {
        return new UltraSearchGraphStore(this, this.database(), store);
    }

    @Override
    public void clear() throws BackendException {
        this.checkOpened();
        /*
         * We should drop database once only with stores(schema/graph/system),
         * otherwise it will easily lead to blocking when drop tables
         */
        Iterator<BackendStore> itor = this.stores.values().iterator();
        if (itor.hasNext()) {
            itor.next().clear();
        }
        this.notifyAndWaitEvent(Events.STORE_CLEAR);
    }

    @Override
    public String type() {
        return "ultrasearch";
    }

    @Override
    public String version() {
        /*
         * Versions history:
         * [1.0] HugeGraph-1328: supports backend table version checking
         * [1.1] HugeGraph-1322: add support for full-text search
         * [1.2] #296: support range sortKey feature
         */
        return "1.2";
    }
}
