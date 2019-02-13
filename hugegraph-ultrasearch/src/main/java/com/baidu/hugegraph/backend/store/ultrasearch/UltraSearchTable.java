package com.baidu.hugegraph.backend.store.ultrasearch;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.slf4j.Logger;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.Condition;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.BackendTable;
import com.baidu.hugegraph.backend.store.TableDefine;
import com.baidu.hugegraph.backend.store.ultrasearch.UltraSearchEntryIterator.PageState;
import com.baidu.hugegraph.backend.store.ultrasearch.UltraSearchSessions.Session;
import com.baidu.hugegraph.exception.NotFoundException;
import com.baidu.hugegraph.iterator.ExtendableIterator;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.util.Log;
import com.google.common.collect.ImmutableList;

public abstract class UltraSearchTable
        extends BackendTable<Session, UltraSearchBackendEntry.Row> {

    private static final Logger LOG = Log.logger(UltraSearchStore.class);

    // The template for insert and delete statements
    private String insertTemplate;
    private String deleteTemplate;

    public UltraSearchTable(String table) {
        super(table);
        this.insertTemplate = null;
        this.deleteTemplate = null;
    }

    public abstract TableDefine tableDefine();

    @Override
    public void init(Session session) {
        this.createTable(session, this.tableDefine());
    }

    @Override
    public void clear(Session session) {
        this.dropTable(session);
    }

    public void truncate(Session session) {
        this.truncateTable(session);
    }

    protected void createTable(Session session, TableDefine tableDefine) {
        LOG.info("createTable table: {}", this.table());
    }

    protected void dropTable(Session session) {
        LOG.info("Drop table: {}", this.table());

    }

    protected void truncateTable(Session session) {
        LOG.info("Truncate table: {}", this.table());
    }

    protected List<HugeKeys> idColumnName() {
        return this.tableDefine().keys();
    }

    protected List<Long> idColumnValue(UltraSearchBackendEntry.Row entry) {
        return ImmutableList.of(entry.id().asLong());
    }

    protected List<Object> idColumnValue(Id id) {
        return ImmutableList.of(id.asObject());
    }

    /**
     * Insert an entire row
     */
    @Override
    public void insert(Session session, UltraSearchBackendEntry.Row entry) {
        String docID = session.getDocID(this.table(), entry.id().asString());
        JSONObject obj = new JSONObject();
        obj.put("put", docID);

        Map<String, Object> fields = new HashMap<>();
        for(Map.Entry item : entry.columns().entrySet())
        {
            if(HugeKeys.ENABLE_LABEL_INDEX.name().equalsIgnoreCase(((HugeKeys)item.getKey()).name())){
                if((Boolean) item.getValue()){
                    fields.put(((HugeKeys)item.getKey()).name(), 1);
                }else{
                    fields.put(((HugeKeys)item.getKey()).name(), 0);
                }
            }else{
                fields.put(((HugeKeys)item.getKey()).name(), item.getValue());
            }

        }

        obj.put("fields", fields);
        session.add(docID, obj.toString());

        LOG.info("insert : " + obj.toString());
    }

    protected String buildDeleteTemplate(List<HugeKeys> idNames, List<Object> values) {
        if (this.deleteTemplate != null) {
            return this.deleteTemplate;
        }

        StringBuilder delete = new StringBuilder();
        delete.append("select * FROM ").append(this.table());
        this.appendPartition(delete);

        WhereBuilder where = new WhereBuilder();
        where.and(formatKeys(idNames), values);
        delete.append(where.build());

        this.deleteTemplate = delete.toString();
        return this.deleteTemplate;
    }

    @Override
    public void delete(Session session, UltraSearchBackendEntry.Row entry) {
        List<HugeKeys> idNames = this.idColumnName();
        List<Object> values = new ArrayList<>();
        List<HugeKeys> realIDNames = new ArrayList<>();

        if (entry.columns().isEmpty()) {
            // Delete just by id
            List<Long> idValues = this.idColumnValue(entry);
            assert idNames.size() == idValues.size();

            for (int i = 0, n = idNames.size(); i < n; i++) {
                if(null == idValues.get(i)) continue;

                values.add(idValues.get(i));
                realIDNames.add(idNames.get(i));
            }

            String sql = this.buildDeleteTemplate(realIDNames, values);
            session.deleteWhere(sql);
        } else {
            for (int i = 0, n = idNames.size(); i < n; i++) {

                HugeKeys key = idNames.get(i);
                Object value = entry.column(key);

                if(null == value) continue;

                values.add(value);
                realIDNames.add(idNames.get(i));
            }

            String sql = this.buildDeleteTemplate(realIDNames, values);
            session.deleteWhere(sql);
        }
    }

    @Override
    public void append(Session session, UltraSearchBackendEntry.Row entry) {
        this.insert(session, entry);
    }

    @Override
    public void eliminate(Session session, UltraSearchBackendEntry.Row entry) {
        this.delete(session, entry);
    }

    @Override
    public Iterator<BackendEntry> query(Session session, Query query) {
        ExtendableIterator<BackendEntry> rs = new ExtendableIterator<>();

        if (query.limit() == 0 && query.limit() != Query.NO_LIMIT) {
            LOG.info("Return empty result(limit=0) for query {}", query);
            return rs;
        }

        LOG.info("query here1");

        List<StringBuilder> selections = this.query2Select(this.table(), query);

        LOG.info("query here2");

        for (StringBuilder selection : selections) {
            LOG.info("query selection:" + selection.toString());

            JSONArray results = session.select(selection.toString());
            rs.extend(this.results2Entries(query, results));
        }

        LOG.info("Return {} for query {}", rs, query);
        return rs;
    }

    protected List<StringBuilder> query2Select(String table, Query query) {
        // Set table
        StringBuilder select = new StringBuilder(64);
        select.append("SELECT * FROM ").append(table);

        // Is query by id?
        List<StringBuilder> ids = this.queryId2Select(query, select);

        List<StringBuilder> selections;

        if (query.conditions().isEmpty()) {
            // Query only by id
            LOG.info("Query only by id(s): {}", ids);
            selections = ids;
        } else {
            selections = new ArrayList<>(ids.size());
            for (StringBuilder selection : ids) {
                // Query by condition
                selections.addAll(this.queryCondition2Select(query, selection));
            }
            LOG.info("Query by conditions: {}", selections);
        }
        // Set page, order-by and limit
        for (StringBuilder selection : selections) {
            if (!query.orders().isEmpty()) {
                this.wrapOrderBy(selection, query);
            }
            if (query.paging()) {
                this.wrapPage(selection, query);
            } else if (query.limit() != Query.NO_LIMIT || query.offset() > 0) {
                this.wrapOffset(selection, query);
            }
        }

        return selections;
    }

    protected List<StringBuilder> queryId2Select(Query query,
                                                 StringBuilder select) {
        // Query by id(s)
        if (query.ids().isEmpty()) {
            return ImmutableList.of(select);
        }

        List<HugeKeys> nameParts = this.idColumnName();

        List<List<Object>> ids = new ArrayList<>(query.ids().size());
        for (Id id : query.ids()) {
            List<Object> idParts = this.idColumnValue(id);
            if (nameParts.size() != idParts.size()) {
                throw new NotFoundException(
                        "Unsupported ID format: '%s' (should contain %s)",
                        id, nameParts);
            }
            ids.add(idParts);
        }

        // Query only by partition-key
        if (nameParts.size() == 1) {
            List<Object> values = new ArrayList<>(ids.size());
            for (List<Object> objects : ids) {
                assert objects.size() == 1;
                values.add(objects.get(0));
            }

            WhereBuilder where = new WhereBuilder();
            where.in(formatKey(nameParts.get(0)), values);
            select.append(where.build());
            return ImmutableList.of(select);
        }

        /*
         * Query by partition-key + clustering-key
         * NOTE: Error if multi-column IN clause include partition key:
         * error: multi-column relations can only be applied to clustering
         * columns when using: select.where(QueryBuilder.in(names, idList));
         * So we use multi-query instead of IN
         */
        List<StringBuilder> selections = new ArrayList<>(ids.size());
        for (List<Object> objects : ids) {
            assert nameParts.size() == objects.size();
            StringBuilder idSelection = new StringBuilder(select);
            /*
             * NOTE: concat with AND relation, like:
             * "pk = id and ck1 = v1 and ck2 = v2"
             */
            WhereBuilder where = new WhereBuilder();
            where.and(formatKeys(nameParts), objects);

            idSelection.append(where.build());
            selections.add(idSelection);
        }
        return selections;
    }

    protected List<StringBuilder> queryCondition2Select(Query query,
                                                        StringBuilder select) {
        // Query by conditions
        Set<Condition> conditions = query.conditions();
        List<StringBuilder> clauses = new ArrayList<>(conditions.size());
        for (Condition condition : conditions) {
            clauses.add(this.condition2Sql(condition));
        }
        WhereBuilder where = new WhereBuilder();
        where.and(clauses);
        select.append(where.build());
        return ImmutableList.of(select);
    }

    protected StringBuilder condition2Sql(Condition condition) {
        switch (condition.type()) {
            case AND:
                Condition.And and = (Condition.And) condition;
                StringBuilder left = this.condition2Sql(and.left());
                StringBuilder right = this.condition2Sql(and.right());
                int size = left.length() + right.length() + " AND ".length();
                StringBuilder sql = new StringBuilder(size);
                sql.append(left).append(" AND ").append(right);
                return sql;
            case OR:
                throw new BackendException("Not support OR currently");
            case RELATION:
                Condition.Relation r = (Condition.Relation) condition;
                return this.relation2Sql(r);
            default:
                final String msg = "Unsupported condition: " + condition;
                throw new AssertionError(msg);
        }
    }

    protected StringBuilder relation2Sql(Condition.Relation relation) {
        String key = relation.serialKey().toString();
        Object value = relation.serialValue();

        // Serialize value (TODO: should move to Serializer)
        value = serializeValue(value);

        StringBuilder sql = new StringBuilder(32);
        sql.append(key);
        switch (relation.relation()) {
            case EQ:
                if (value instanceof String){
                    sql.append(" contains ").append(value);
                }else{
                    sql.append(" = ").append(value);
                }
                break;
            case NEQ:
                sql.append(" != ").append(value);
                break;
            case GT:
                sql.append(" > ").append(value);
                break;
            case GTE:
                sql.append(" >= ").append(value);
                break;
            case LT:
                sql.append(" < ").append(value);
                break;
            case LTE:
                sql.append(" <= ").append(value);
                break;
            case IN:
                List<?> values = (List<?>) value;
                if(1 == values.size()){
                    sql.append(key).append(" ");
                    Object v = values.get(0);
                    if (v instanceof String) {
                        sql.append(" contains " + UltraSearchUtil.escapeString((String) v));
                    } else {
                        sql.append(" = " + v);
                    }
                }else if(1 < values.size()){
                    sql.append(key).append(" contains equiv (");
                    for (int i = 0, n = values.size(); i < n; i++) {
                        Object v = values.get(i);
                        if (v instanceof String) {
                            sql.append(UltraSearchUtil.escapeString((String) v));
                        } else {
                            sql.append("'" + v + "'");
                        }
                        if (i != n - 1) {
                            sql.append(", ");
                        }
                    }
                    sql.append(")");
                }
                break;
            case CONTAINS:
            case CONTAINS_KEY:
            case SCAN:
            default:
                throw new AssertionError("Unsupported relation: " + relation);
        }
        return sql;
    }

    protected void wrapOrderBy(StringBuilder select, Query query) {
        int size = query.orders().size();
        assert size > 0;

        int i = 0;
        // Set order-by
        select.append(" ORDER BY ");
        for (Map.Entry<HugeKeys, Query.Order> order :
                query.orders().entrySet()) {
            String key = formatKey(order.getKey());
            Query.Order value = order.getValue();
            select.append(key).append(" ");
            if (value == Query.Order.ASC) {
                select.append("ASC");
            } else {
                assert value == Query.Order.DESC;
                select.append("DESC");
            }
            if (++i != size) {
                select.append(", ");
            }
        }
    }

    protected void wrapPage(StringBuilder select, Query query) {
        String page = query.page();
        // It's the first time if page is empty
        if (!page.isEmpty()) {
            PageState pageState = PageState.fromString(page);
            Map<HugeKeys, Object> columns = pageState.columns();

            List<HugeKeys> idColumnNames = this.idColumnName();
            List<Object> values = new ArrayList<>(idColumnNames.size());
            for (HugeKeys key : idColumnNames) {
                values.add(columns.get(key));
            }

            // Need add `where` to `select` when query is IdQuery
            boolean startWithWhere = query.conditions().isEmpty();
            WhereBuilder where = new WhereBuilder(startWithWhere);
            where.gte(formatKeys(idColumnNames), values);
            select.append(where.build());
        }

        assert query.limit() != Query.NO_LIMIT;
        // Fetch `limit + 1` records for judging whether reached the last page
        select.append(" limit ");
        select.append(query.limit() + 1);
    }

    protected void wrapOffset(StringBuilder select, Query query) {
        assert query.limit() >= 0;
        assert query.offset() >= 0;
        // Set limit and offset
        select.append(" limit ");
        select.append(query.limit());
        select.append(" offset ");
        select.append(query.offset());
    }

    private static Object serializeValue(Object value) {
        if (value instanceof Id) {
            value = ((Id) value).asObject();
        }
        if (value instanceof String) {
            value = UltraSearchUtil.escapeString((String) value);
        }
        return value;
    }

    protected Iterator<BackendEntry> results2Entries(Query query,
                                                     JSONArray results) {
        return new UltraSearchEntryIterator(results, query, this::mergeEntries);
    }

    protected BackendEntry mergeEntries(BackendEntry e1, BackendEntry e2) {
        // Return the next entry (not merged)
        return e2;
    }

    protected void appendPartition(StringBuilder delete) {
        // pass
    }

    public static String formatKey(HugeKeys key) {
        return key.name();
    }

    public static HugeKeys parseKey(String name) {
        return HugeKeys.valueOf(name.toUpperCase());
    }

    public static List<String> formatKeys(List<HugeKeys> keys) {
        List<String> names = new ArrayList<>(keys.size());
        for (HugeKeys key : keys) {
            names.add(formatKey(key));
        }
        return names;
    }
}
