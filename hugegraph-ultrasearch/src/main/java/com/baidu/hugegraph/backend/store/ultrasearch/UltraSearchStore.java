package com.baidu.hugegraph.backend.store.ultrasearch;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.store.AbstractBackendStore;
import com.baidu.hugegraph.backend.store.BackendAction;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.BackendFeatures;
import com.baidu.hugegraph.backend.store.BackendMutation;
import com.baidu.hugegraph.backend.store.BackendStoreProvider;
import com.baidu.hugegraph.backend.store.ultrasearch.UltraSearchSessions.Session;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;

public abstract class UltraSearchStore extends AbstractBackendStore<Session> {

    private static final Logger LOG = Log.logger(UltraSearchStore.class);

    private static final BackendFeatures FEATURES = new UltraSearchFeatures();

    private final String store;
    private final String database;

    private final BackendStoreProvider provider;

    private final Map<HugeType, UltraSearchTable> tables;

    private UltraSearchSessions sessions;

    public UltraSearchStore(final BackendStoreProvider provider,
                      final String database, final String store) {
        E.checkNotNull(database, "database");
        E.checkNotNull(store, "store");
        this.provider = provider;
        this.database = database;
        this.store = store;

        this.sessions = null;
        this.tables = new ConcurrentHashMap<>();

        LOG.debug("Store loaded: {}", store);
    }

    protected void registerTableManager(HugeType type, UltraSearchTable table) {
        this.tables.put(type, table);
    }

    protected UltraSearchSessions openSessionPool(HugeConfig config) {
        return new UltraSearchSessions(config, this.database, this.store);
    }

    @Override
    public String store() {
        return this.store;
    }

    @Override
    public String database() {
        return this.database;
    }

    @Override
    public BackendStoreProvider provider() {
        return this.provider;
    }

    @Override
    public synchronized void open(HugeConfig config) {
        LOG.debug("Store open: {}", this.store);

        E.checkNotNull(config, "config");

        if (this.sessions != null && !this.sessions.closed()) {
            LOG.debug("Store {} has been opened before", this.store);
            this.sessions.useSession();
            return;
        }

        this.sessions = this.openSessionPool(config);

        LOG.debug("Store connect with database: {}", this.database);
        try {
            this.sessions.open(config);
        } catch (Exception e) {
            if (!e.getMessage().startsWith("Unknown database")) {
                throw new BackendException("Failed connect with ultrasearch, " +
                        "please ensure it's ok", e);
            }
            LOG.info("Failed to open database '{}', " +
                    "try to init database later", this.database);
        }

        try {
            this.sessions.session();
        } catch (Throwable e) {
            try {
                this.sessions.close();
            } catch (Throwable e2) {
                LOG.warn("Failed to close connection after an error", e2);
            }
            throw new BackendException("Failed to open database", e);
        }

        LOG.debug("Store opened: {}", this.store);
    }

    @Override
    public void close() {
        LOG.debug("Store close: {}", this.store);
        this.checkClusterConnected();
        this.sessions.close();
    }

    @Override
    public void init() {
        this.checkClusterConnected();

        this.sessions.session().open();

        this.checkSessionConnected();
        this.initTables();

        LOG.debug("Store initialized: {}", this.store);
    }

    @Override
    public void clear() {
        // Check connected
        this.checkClusterConnected();
        LOG.debug("Store cleared: {}", this.store);
    }

    @Override
    public void truncate() {
        this.checkSessionConnected();

        this.truncateTables();
        LOG.debug("Store truncated: {}", this.store);
    }

    @Override
    public void mutate(BackendMutation mutation) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Store {} mutation: {}", this.store, mutation);
        }

        this.checkSessionConnected();
        Session session = this.sessions.session();

        for (Iterator<BackendAction> it = mutation.mutation(); it.hasNext();) {
            this.mutate(session, it.next());
        }
    }

    private void mutate(Session session, BackendAction item) {
        UltraSearchBackendEntry entry = castBackendEntry(item.entry());
        UltraSearchTable table = this.table(entry.type());

        switch (item.action()) {
            case INSERT:
                table.insert(session, entry.row());
                break;
            case DELETE:
                table.delete(session, entry.row());
                break;
            case APPEND:
                table.append(session, entry.row());
                break;
            case ELIMINATE:
                table.eliminate(session, entry.row());
                break;
            default:
                throw new AssertionError(String.format(
                        "Unsupported mutate action: %s", item.action()));
        }
    }

    @Override
    public Iterator<BackendEntry> query(Query query) {
        this.checkSessionConnected();

        UltraSearchTable table = this.table(UltraSearchTable.tableType(query));
        return table.query(this.sessions.session(), query);
    }

    @Override
    public void beginTx() {
        throw new BackendException("beginTx not suport");
    }

    @Override
    public void commitTx() {
        throw new BackendException("beginTx not Unsupported");
    }

    @Override
    public void rollbackTx() {
        throw new BackendException("beginTx not Unsupported");
    }

    @Override
    public BackendFeatures features() {
        return FEATURES;
    }

    @Override
    public String toString() {
        return this.store;
    }

    protected void initTables() {
        Session session = this.sessions.session();
        for (UltraSearchTable table : this.tables()) {
            table.init(session);
        }
    }

    protected void clearTables() {
        Session session = this.sessions.session();
        for (UltraSearchTable table : this.tables()) {
            table.clear(session);
        }
    }

    protected void truncateTables() {
        Session session = this.sessions.session();
        for (UltraSearchTable table : this.tables()) {
            table.truncate(session);
        }
    }

    protected Collection<UltraSearchTable> tables() {
        return this.tables.values();
    }

    @Override
    protected final UltraSearchTable table(HugeType type) {
        assert type != null;
        UltraSearchTable table = this.tables.get(type);
        if (table == null) {
            throw new BackendException("Unsupported table type: %s", type);
        }
        return table;
    }

    @Override
    protected Session session(HugeType type) {
        this.checkSessionConnected();
        return this.sessions.session();
    }

    protected final void checkClusterConnected() {
        E.checkState(this.sessions != null,
                "MySQL store has not been initialized");
    }

    protected final void checkSessionConnected() {
        this.checkClusterConnected();
        this.sessions.checkSessionConnected();
    }

    protected static UltraSearchBackendEntry castBackendEntry(BackendEntry entry) {
        if (!(entry instanceof UltraSearchBackendEntry)) {
            throw new BackendException(
                    "MySQL store only supports UltraSearchBackendEntry");
        }
        return (UltraSearchBackendEntry) entry;
    }

    public static class UltraSearchSchemaStore extends UltraSearchStore {

        private final UltraSearchTables.Counters counters;

        public UltraSearchSchemaStore(BackendStoreProvider provider,
                                String database, String store) {
            super(provider, database, store);

            this.counters = new UltraSearchTables.Counters();

            registerTableManager(HugeType.VERTEX_LABEL,
                    new UltraSearchTables.VertexLabel());
            registerTableManager(HugeType.EDGE_LABEL,
                    new UltraSearchTables.EdgeLabel());
            registerTableManager(HugeType.PROPERTY_KEY,
                    new UltraSearchTables.PropertyKey());
            registerTableManager(HugeType.INDEX_LABEL,
                    new UltraSearchTables.IndexLabel());
        }

        @Override
        protected Collection<UltraSearchTable> tables() {
            List<UltraSearchTable> tables = new ArrayList<>(super.tables());
            tables.add(this.counters);
            return tables;
        }

        @Override
        public void increaseCounter(HugeType type, long increment) {
            this.checkSessionConnected();
            Session session = super.sessions.session();
            this.counters.increaseCounter(session, type, increment);
        }

        @Override
        public long getCounter(HugeType type) {
            this.checkSessionConnected();
            Session session = super.sessions.session();
            return this.counters.getCounter(session, type);
        }
    }

    public static class UltraSearchGraphStore extends UltraSearchStore {

        public UltraSearchGraphStore(BackendStoreProvider provider,
                               String database, String store) {
            super(provider, database, store);

            registerTableManager(HugeType.VERTEX,
                    new UltraSearchTables.Vertex(store));

            registerTableManager(HugeType.EDGE_OUT,
                    UltraSearchTables.Edge.out(store));
            registerTableManager(HugeType.EDGE_IN,
                    UltraSearchTables.Edge.in(store));

            registerTableManager(HugeType.SECONDARY_INDEX,
                    new UltraSearchTables.SecondaryIndex(store));
            registerTableManager(HugeType.RANGE_INDEX,
                    new UltraSearchTables.RangeIndex(store));
            registerTableManager(HugeType.SEARCH_INDEX,
                    new UltraSearchTables.SearchIndex(store));
        }

        @Override
        public Id nextId(HugeType type) {
            throw new UnsupportedOperationException("UltraSearchGraphStore.nextId()");
        }

        @Override
        public void increaseCounter(HugeType type, long num) {
            throw new UnsupportedOperationException(
                    "UltraSearchGraphStore.increaseCounter()");
        }

        @Override
        public long getCounter(HugeType type) {
            throw new UnsupportedOperationException(
                    "UltraSearchGraphStore.getCounter()");
        }
    }
}
