package com.baidu.hugegraph.backend.store.ultrasearch;

import java.net.SocketTimeoutException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.apache.http.client.utils.URIBuilder;
import org.slf4j.Logger;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.store.BackendSession;
import com.baidu.hugegraph.backend.store.BackendSessionPool;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;

import com.ultracloud.ultrasearch.http.client.FeedClient;
import com.ultracloud.ultrasearch.http.client.FeedClientFactory;
import com.ultracloud.ultrasearch.http.client.SimpleLoggerResultCallback;
import com.ultracloud.ultrasearch.http.client.config.Cluster;
import com.ultracloud.ultrasearch.http.client.config.ConnectionParams;
import com.ultracloud.ultrasearch.http.client.config.Endpoint;
import com.ultracloud.ultrasearch.http.client.config.FeedParams;
import com.ultracloud.ultrasearch.http.client.config.SessionParams;

public class UltraSearchSessions extends BackendSessionPool {

    private static final Logger LOG = Log.logger(UltraSearchSessions.class);

    private static final int DROP_DB_TIMEOUT = 10000;

    private HugeConfig config;
    private String database;
    private boolean opened;

    public UltraSearchSessions(HugeConfig config, String database, String store) {
        //super(database + "/" + store);
        super(database);
        this.config = config;
        this.database = database;
        this.opened = false;
    }

    public HugeConfig config() {
        return this.config;
    }

    public String database() {
        return this.database;
    }

    /**
     * Try connect with specified database, will not reconnect if failed
     * @throws SQLException if a database access error occurs
     */
    @Override
    public void open(HugeConfig config) throws Exception {
        try (Connection conn = this.open(false)) {
            this.opened = true;
        }
    }

    @Override
    protected boolean opened() {
        return this.opened;
    }

    /**
     * Connect DB with specified database
     */
    private Connection open(boolean autoReconnect) throws SQLException {
        String url = this.config.get(UltraSearchOptions.JDBC_URL);
        if (url.endsWith("/")) {
            url = String.format("%s%s", url, this.database);
        } else {
            url = String.format("%s/%s", url, this.database);
        }

        int maxTimes = this.config.get(UltraSearchOptions.JDBC_RECONNECT_MAX_TIMES);
        int interval = this.config.get(UltraSearchOptions.JDBC_RECONNECT_INTERVAL);

        URIBuilder uriBuilder = new URIBuilder();
        uriBuilder.setPath(url)
                .setParameter("rewriteBatchedStatements", "true")
                .setParameter("useServerPrepStmts", "false")
                .setParameter("autoReconnect", String.valueOf(autoReconnect))
                .setParameter("maxReconnects", String.valueOf(maxTimes))
                .setParameter("initialTimeout", String.valueOf(interval));
        return this.connect(uriBuilder.toString());
    }

    private Connection connect(String url) throws SQLException {
        String driverName = this.config.get(UltraSearchOptions.JDBC_DRIVER);
        String username = this.config.get(UltraSearchOptions.JDBC_USERNAME);
        String password = this.config.get(UltraSearchOptions.JDBC_PASSWORD);
        try {
            // Register JDBC driver
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            throw new BackendException("Invalid driver class '%s'",
                    driverName);
        }
        return DriverManager.getConnection(url, username, password);
    }

    @Override
    protected synchronized Session newSession() {
        return new Session();
    }

    @Override
    protected void doClose() {
        // pass
    }

    @Override
    public synchronized Session session() {
        return (Session) super.getOrNewSession();
    }

    public void checkSessionConnected() {
        Session session = this.session();
        E.checkState(session != null, "MySQL session has not been initialized");
        E.checkState(!session.closed(), "MySQL session has been closed");
    }

    public void createDatabase() {
        // Create database with non-database-session
        LOG.debug("Create database: {}", this.database);

        String sql = this.buildCreateDatabase(this.database);
        try (Connection conn = this.openWithoutDB(0)) {
            conn.createStatement().execute(sql);
        } catch (SQLException e) {
            throw new BackendException("Failed to create database '%s'",
                    this.database);
        }
    }

    protected String buildCreateDatabase(String database) {
        return String.format("CREATE DATABASE IF NOT EXISTS %s " +
                        "DEFAULT CHARSET utf8 COLLATE utf8_bin;",
                database);
    }

    public void dropDatabase() {
        LOG.debug("Drop database: {}", this.database);

        String sql = String.format("DROP DATABASE IF EXISTS %s;",
                this.database);
        try (Connection conn = this.openWithoutDB(DROP_DB_TIMEOUT)) {
            conn.createStatement().execute(sql);
        } catch (SQLException e) {
            if (e.getCause() instanceof SocketTimeoutException) {
                LOG.warn("Drop database '{}' timeout", this.database);
            } else {
                throw new BackendException("Failed to drop database '%s'",
                        this.database);
            }
        }
    }

    public boolean existsDatabase() {
        try (Connection conn = this.openWithoutDB(0);
             ResultSet result = conn.getMetaData().getCatalogs()) {
            while (result.next()) {
                String dbName = result.getString(1);
                if (dbName.equals(this.database)) {
                    return true;
                }
            }
        } catch (Exception e) {
            throw new BackendException("Failed to obtain MySQL metadata, " +
                    "please ensure it is ok", e);
        }
        return false;
    }

    /**
     * Connect DB without specified database
     */
    private Connection openWithoutDB(int timeout) {
        String jdbcUrl = this.config.get(UltraSearchOptions.JDBC_URL);
        String url = new URIBuilder().setPath(jdbcUrl)
                .setParameter("socketTimeout",
                        String.valueOf(timeout))
                .toString();
        try {
            return connect(url);
        } catch (SQLException e) {
            throw new BackendException("Failed to access %s, " +
                    "please ensure it is ok", jdbcUrl);
        }
    }

    public static class Operation {
        final public String documentId;
        final public CharSequence data;

        public Operation(String id, CharSequence data) {
            this.documentId = id;
            this.data = data;
        }
    }

    public class Session extends BackendSession {

        //private Connection conn;
        //private Map<String, PreparedStatement> statements;
        private BlockingQueue<Operation> operations = new ArrayBlockingQueue<Operation>(100);
        private FeedClient feedClient;
        private AtomicInteger pending = new AtomicInteger(0);
        private boolean opened;
        private int count;

        public Session() {
            this.conn = null;
            this.statements = new HashMap<>();
            this.opened = false;
            this.count = 0;
            try {
                this.open();
            } catch (SQLException ignored) {
                // Ignore
            }
        }

        public void open(){
            Endpoint endPoint = Endpoint.create(config.get(UltraSearchOptions.JDBC_PASSWORD), 6808, false);
            SessionParams sessionParams = new SessionParams.Builder()
                    .addCluster(new Cluster.Builder().addEndpoint(endPoint).build())
                    .setConnectionParams(new ConnectionParams.Builder().setDryRun(false).build())
                    .setFeedParams(new FeedParams.Builder()
                            .setDataFormat(FeedParams.DataFormat.JSON_UTF8)
                            .build())
                    .build();

            this.feedClient = FeedClientFactory.create(sessionParams, new SimpleLoggerResultCallback(this.pending, 10));
        }

        @Override
        public void close() {
            assert this.closeable();
            if (this.conn == null) {
                return;
            }

            SQLException exception = null;
            for (PreparedStatement statement : this.statements.values()) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    exception = e;
                }
            }

            try {
                this.conn.close();
            } catch (SQLException e) {
                exception = e;
            }

            this.opened = false;
            if (exception != null) {
                throw new BackendException("Failed to close connection",
                        exception);
            }
        }

        @Override
        public boolean closed() {
            return !this.opened;
        }

        @Override
        public void clear() {
            this.count = 0;
            SQLException exception = null;
            for (PreparedStatement statement : this.statements.values()) {
                try {
                    statement.clearBatch();
                } catch (SQLException e) {
                    exception = e;
                }
            }
            if (exception != null) {
                /*
                 * Will throw exception when the database connection error,
                 * we clear statements because clearBatch() failed
                 */
                this.statements = new HashMap<>();
            }
        }

        @Override
        public Integer commit() {
            throw new RuntimeException("Not supported by US");
        }

        @Override
        public boolean hasChanges() {
            return this.count > 0;
        }

        public ResultSet select(String sql) throws SQLException {
            assert this.conn.getAutoCommit();
            return this.conn.createStatement().executeQuery(sql);
        }

        public void add(PreparedStatement statement) {
            try {
                // Add a row to statement
                statement.addBatch();
                this.count++;
            } catch (SQLException e) {
                throw new BackendException("Failed to add statement '%s' " +
                        "to batch", e, statement);
            }
        }

        public PreparedStatement prepareStatement(String sqlTemplate)
                throws SQLException {
            PreparedStatement statement = this.statements.get(sqlTemplate);
            if (statement == null) {
                statement = this.conn.prepareStatement(sqlTemplate);
                this.statements.putIfAbsent(sqlTemplate, statement);
            }
            return statement;
        }
    }
}
