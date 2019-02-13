package com.baidu.hugegraph.backend.store.ultrasearch;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import net.sf.json.JSONObject;
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

        LOG.info("Store loaded: {}", store);
    }

    protected void registerTableManager(HugeType type, UltraSearchTable table) {
        this.tables.put(type, table);
    }

    protected UltraSearchSessions openSessionPool(HugeConfig config) {
        LOG.info("openSessionPool");

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
        LOG.info("Store open: {}", this.store);

        E.checkNotNull(config, "config");

        if (this.sessions != null && !this.sessions.closed()) {
            LOG.info("Store {} has been opened before", this.store);
            this.sessions.useSession();
            return;
        }

        this.sessions = this.openSessionPool(config);

        LOG.info("Store connect with database: {}", this.database);
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

        LOG.info("Store opened: {}", this.store);
    }

    //临时处理，initStore解决后去掉
    void initStore(UltraSearchSessions.Session session){
        JSONObject result = session.get("counters", "SYS_SCHEMA");
        if(null != result){
            return;
        }

        //property_keys
        insertPropertyKeys(session, -15, "~backend_info", 8, "{\"version\":\"1.2\"}");
        insertPropertyKeys(session, -12, "~task_dependencies", 5, "{}");
        insertPropertyKeys(session, -11, "~task_result", 8, "{}");
        insertPropertyKeys(session, -10, "~task_input", 8, "{}");
        insertPropertyKeys(session, -9, "~task_retries", 4, "{}");
        insertPropertyKeys(session, -8, "~task_update", 10, "{}");
        insertPropertyKeys(session, -7, "~task_create", 10, "{}");
        insertPropertyKeys(session, -6, "~task_progress", 4, "{}");
        insertPropertyKeys(session, -5, "~task_status", 3, "{}");
        insertPropertyKeys(session, -4, "~task_description", 8, "{}");
        insertPropertyKeys(session, -3, "~task_callable", 8, "{}");
        insertPropertyKeys(session, -2, "~task_name", 8, "{}");
        insertPropertyKeys(session, -1, "~task_type", 8, "{}");

        //vertex_labels
        {
            JSONObject obj = new JSONObject();

            Map<String, Object> fieldsMap = new HashMap<>();
            fieldsMap.put("ID", -13);
            fieldsMap.put("NAME", "~task");
            fieldsMap.put("ID_STRATEGY", 4);
            fieldsMap.put("PRIMARY_KEYS", "[]");
            fieldsMap.put("PROPERTIES", "[-1,-2,-3,-4,-5,-6,-7,-8,-9,-10,-11,-12]");
            fieldsMap.put("NULLABLE_KEYS", "[-4,-8,-10,-11,-12]");
            fieldsMap.put("INDEX_LABELS", "[-14]");
            fieldsMap.put("ENABLE_LABEL_INDEX", 1);
            fieldsMap.put("USER_DATA", "{}");
            fieldsMap.put("STATUS", 1);
            obj.put("fields", fieldsMap);

            session.postDoc("vertex_labels", "" + -13, obj.toString());
        }

        //index_labels
        {
            JSONObject obj = new JSONObject();

            Map<String, Object> fields = new HashMap<>();
            fields.put("ID", -14);
            fields.put("NAME", "~task-index-by-~task_status");
            fields.put("BASE_TYPE", 1);
            fields.put("BASE_VALUE", -13);
            fields.put("INDEX_TYPE", 1);
            fields.put("FIELDS", "[-5]");
            fields.put("STATUS", 1);

            obj.put("fields", fields);
            session.postDoc("index_labels", "" + -14, obj.toString());
        }

        UltraSearchTables.Counters.putCounter(session, HugeType.SYS_SCHEMA, 16);

        LOG.info("init store success!");
        System.exit(0);
    }

    void insertPropertyKeys(UltraSearchSessions.Session session, int id, String name, int dataType, String userData){
        JSONObject obj = new JSONObject();

        Map<String, Object> fields = new HashMap<>();
        fields.put("ID", id);
        fields.put("NAME", name);
        fields.put("DATA_TYPE", dataType);
        fields.put("CARDINALITY", 1);
        fields.put("PROPERTIES", "[]");
        fields.put("USER_DATA", userData);
        fields.put("STATUS", 1);

        obj.put("fields", fields);
        session.postDoc("property_keys", "" + id, obj.toString());
    }

    @Override
    public void close() {
        LOG.info("Store close: {}", this.store);
        this.checkClusterConnected();
        this.sessions.close();
    }

    @Override
    public void init() {
        this.checkClusterConnected();

        this.sessions.session().open();

        this.checkSessionConnected();
        this.initTables();

        initStore(this.sessions.session());

        LOG.info("Store initialized: {}", this.store);
    }

    @Override
    public void clear() {
        // Check connected
        this.checkClusterConnected();
        LOG.info("Store cleared: {}", this.store);
    }

    @Override
    public void truncate() {
        this.checkSessionConnected();

        this.truncateTables();
        LOG.info("Store truncated: {}", this.store);
    }

    @Override
    public void mutate(BackendMutation mutation) {
        LOG.info("mutate");

        if (LOG.isDebugEnabled()) {
            LOG.info("Store {} mutation: {}", this.store, mutation);
        }

        this.checkSessionConnected();
        Session session = this.sessions.session();

        for (Iterator<BackendAction> it = mutation.mutation(); it.hasNext();) {
            this.mutate(session, it.next());
        }
    }

    private void mutate(Session session, BackendAction item) {
        LOG.info("mutate");

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
        LOG.info("query");

        this.checkSessionConnected();

        LOG.info("query here10");

        UltraSearchTable table = this.table(UltraSearchTable.tableType(query));

        LOG.info("query here11");

        return table.query(this.sessions.session(), query);
    }

    @Override
    public void beginTx() {
        //throw new BackendException("beginTx not suport");
    }

    @Override
    public void commitTx() {
        //throw new BackendException("beginTx not Unsupported");
    }

    @Override
    public void rollbackTx() {
        //throw new BackendException("beginTx not Unsupported");
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
        LOG.info("initTables");

        Session session = this.sessions.session();
        for (UltraSearchTable table : this.tables()) {
            table.init(session);
        }
    }

    protected void clearTables() {
        LOG.info("clearTables");

        Session session = this.sessions.session();
        for (UltraSearchTable table : this.tables()) {
            table.clear(session);
        }
    }

    protected void truncateTables() {
        LOG.info("truncateTables");

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
