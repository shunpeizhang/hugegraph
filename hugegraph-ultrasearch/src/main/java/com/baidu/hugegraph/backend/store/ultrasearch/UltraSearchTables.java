

package com.baidu.hugegraph.backend.store.ultrasearch;



import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.id.EdgeId;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.backend.id.IdUtil;
import com.baidu.hugegraph.backend.id.SplicingIdGenerator;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.TableDefine;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.util.E;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import net.sf.json.JSONObject;

public class UltraSearchTables {

    private static final String INT = "int";

    private static final String DATATYPE_PK = "int";
    private static final String DATATYPE_SL = "int"; // VL/EL
    private static final String DATATYPE_IL = "int";

    private static final String BOOLEAN = "byte";
    private static final String TINYINT = "byte";
    private static final String DOUBLE = "double";
    private static final String VARCHAR = "string";
    private static final String SMALL_JSON = "string";
    private static final String LARGE_JSON = "string";

    public static class UltraSearchTableTemplate extends UltraSearchTable {

        protected TableDefine define;

        public UltraSearchTableTemplate(String table) {
            super(table);
        }

        @Override
        public TableDefine tableDefine() {
            return this.define;
        }
    }

    public static class Counters extends UltraSearchTableTemplate {

        public static final String TABLE = "counters";

        public Counters() {
            super(TABLE);

            this.define = new TableDefine();
            this.define.column(HugeKeys.SCHEMA_TYPE, VARCHAR);
            this.define.column(HugeKeys.ID, INT);
            this.define.keys(HugeKeys.SCHEMA_TYPE);
        }

        public long getCounter(UltraSearchSessions.Session session, HugeType type) {
            String schemaCol = formatKey(HugeKeys.SCHEMA_TYPE);
            String idCol = formatKey(HugeKeys.ID);

            JSONObject result = session.get(TABLE, type.name());
            if(null == result){
                return 0L;
            }
            return result.getInt("id");
        }

        public void increaseCounter(UltraSearchSessions.Session session,
                                    HugeType type, long increment) {

            if(0 == getCounter(session, type)){
                String docID = session.getDocID(TABLE, type.name());
                JSONObject obj = new JSONObject();
                obj.put("put", docID);

                JSONObject fields = new JSONObject();
                fields.put("ID", 1);
                fields.put("SCHEMA_TYPE", type.name());

                obj.put("fields", fields);
                session.add(docID, obj.toString());
            }else{
                String docID = session.getDocID(TABLE, type.name());
                JSONObject obj = new JSONObject();
                obj.put("update", docID);

                JSONObject fields = new JSONObject();
                JSONObject id = new JSONObject();
                id.put("increment", increment);
                fields.put("ID", id);

                obj.put("fields", fields);
                session.add(docID, obj.toString());
            }
        }
    }

    public static class VertexLabel extends UltraSearchTableTemplate {

        public static final String TABLE = "vertex_labels";

        public VertexLabel() {
            super(TABLE);

            this.define = new TableDefine();
            this.define.column(HugeKeys.ID, DATATYPE_SL);
            this.define.column(HugeKeys.NAME, VARCHAR);
            this.define.column(HugeKeys.ID_STRATEGY, TINYINT);
            this.define.column(HugeKeys.PRIMARY_KEYS, SMALL_JSON);
            this.define.column(HugeKeys.PROPERTIES, SMALL_JSON);
            this.define.column(HugeKeys.NULLABLE_KEYS, SMALL_JSON);
            this.define.column(HugeKeys.INDEX_LABELS, SMALL_JSON);
            this.define.column(HugeKeys.ENABLE_LABEL_INDEX, BOOLEAN);
            this.define.column(HugeKeys.USER_DATA, LARGE_JSON);
            this.define.column(HugeKeys.STATUS, TINYINT);
            this.define.keys(HugeKeys.ID);
        }
    }

    public static class EdgeLabel extends UltraSearchTableTemplate {

        public static final String TABLE = "edge_labels";

        public EdgeLabel() {
            super(TABLE);

            this.define = new TableDefine();
            this.define.column(HugeKeys.ID, DATATYPE_SL);
            this.define.column(HugeKeys.NAME, VARCHAR);
            this.define.column(HugeKeys.FREQUENCY, TINYINT);
            this.define.column(HugeKeys.SOURCE_LABEL, DATATYPE_SL);
            this.define.column(HugeKeys.TARGET_LABEL, DATATYPE_SL);
            this.define.column(HugeKeys.SORT_KEYS, SMALL_JSON);
            this.define.column(HugeKeys.PROPERTIES, SMALL_JSON);
            this.define.column(HugeKeys.NULLABLE_KEYS, SMALL_JSON);
            this.define.column(HugeKeys.INDEX_LABELS, SMALL_JSON);
            this.define.column(HugeKeys.ENABLE_LABEL_INDEX, BOOLEAN);
            this.define.column(HugeKeys.USER_DATA, LARGE_JSON);
            this.define.column(HugeKeys.STATUS, TINYINT);
            this.define.keys(HugeKeys.ID);
        }
    }

    public static class PropertyKey extends UltraSearchTableTemplate {

        public static final String TABLE = "property_keys";

        public PropertyKey() {
            super(TABLE);

            this.define = new TableDefine();
            this.define.column(HugeKeys.ID, DATATYPE_PK);
            this.define.column(HugeKeys.NAME, VARCHAR);
            this.define.column(HugeKeys.DATA_TYPE, TINYINT);
            this.define.column(HugeKeys.CARDINALITY, TINYINT);
            this.define.column(HugeKeys.PROPERTIES, SMALL_JSON);
            this.define.column(HugeKeys.USER_DATA, LARGE_JSON);
            this.define.column(HugeKeys.STATUS, TINYINT);
            this.define.keys(HugeKeys.ID);
        }
    }

    public static class IndexLabel extends UltraSearchTableTemplate {

        public static final String TABLE = "index_labels";

        public IndexLabel() {
            super(TABLE);

            this.define = new TableDefine();
            this.define.column(HugeKeys.ID, DATATYPE_IL);
            this.define.column(HugeKeys.NAME, VARCHAR);
            this.define.column(HugeKeys.BASE_TYPE, TINYINT);
            this.define.column(HugeKeys.BASE_VALUE, DATATYPE_SL);
            this.define.column(HugeKeys.INDEX_TYPE, TINYINT);
            this.define.column(HugeKeys.FIELDS, SMALL_JSON);
            this.define.column(HugeKeys.STATUS, TINYINT);
            this.define.keys(HugeKeys.ID);
        }
    }

    public static class Vertex extends UltraSearchTableTemplate {

        public static final String TABLE = "vertices";

        public Vertex(String store) {
            super(joinTableName(store, TABLE));

            this.define = new TableDefine();
            this.define.column(HugeKeys.ID, VARCHAR);
            this.define.column(HugeKeys.LABEL, DATATYPE_SL);
            this.define.column(HugeKeys.PROPERTIES, LARGE_JSON);
            this.define.keys(HugeKeys.ID);
        }
    }

    public static class Edge extends UltraSearchTableTemplate {

        public static final String TABLE_PREFIX = "edges";

        private final Directions direction;
        private final String delByLabelTemplate;

        protected Edge(String store, Directions direction) {
            super(joinTableName(store, table(direction)));

            this.direction = direction;
            this.delByLabelTemplate = String.format(
                    "{\"sql\" : \"select * from %s where %s = SSS;\"}",
                    this.table(), formatKey(HugeKeys.LABEL));

            this.define = new TableDefine();
            this.define.column(HugeKeys.OWNER_VERTEX, VARCHAR);
            this.define.column(HugeKeys.DIRECTION, TINYINT);
            this.define.column(HugeKeys.LABEL, DATATYPE_SL);
            this.define.column(HugeKeys.SORT_VALUES, VARCHAR);
            this.define.column(HugeKeys.OTHER_VERTEX, VARCHAR);
            this.define.column(HugeKeys.PROPERTIES, LARGE_JSON);
            this.define.keys(HugeKeys.OWNER_VERTEX, HugeKeys.DIRECTION,
                    HugeKeys.LABEL, HugeKeys.SORT_VALUES,
                    HugeKeys.OTHER_VERTEX);
        }

        @Override
        protected List<Object> idColumnValue(Id id) {
            EdgeId edgeId;
            if (!(id instanceof EdgeId)) {
                String[] idParts = EdgeId.split(id);
                if (idParts.length == 1) {
                    // Delete edge by label
                    return Arrays.asList((Object[]) idParts);
                }
                id = IdUtil.readString(id.asString());
                edgeId = EdgeId.parse(id.asString());
            } else {
                edgeId = (EdgeId) id;
            }

            E.checkState(edgeId.direction() == this.direction,
                    "Can't query %s edges from %s edges table",
                    edgeId.direction(), this.direction);

            List<Object> list = new ArrayList<>(5);
            list.add(IdUtil.writeString(edgeId.ownerVertexId()));
            list.add(edgeId.direction().code());
            list.add(edgeId.edgeLabelId().asLong());
            list.add(edgeId.sortValues());
            list.add(IdUtil.writeString(edgeId.otherVertexId()));
            return list;
        }

        @Override
        public void delete(UltraSearchSessions.Session session, UltraSearchBackendEntry.Row entry) {
            // Let super class do delete if not deleting edge by label
            List<Object> idParts = this.idColumnValue(entry.id());
            if (idParts.size() > 1 || entry.columns().size() > 0) {
                super.delete(session, entry);
                return;
            }

            // The only element is label
            this.deleteEdgesByLabel(session, entry.id());
        }

        private void deleteEdgesByLabel(UltraSearchSessions.Session session, Id label) {
            String sql = this.delByLabelTemplate.replace("SSS", label.asString());
            session.deleteWhere(sql);
        }

        @Override
        protected BackendEntry mergeEntries(BackendEntry e1, BackendEntry e2) {
            // Merge edges into vertex
            // TODO: merge rows before calling row2Entry()

            UltraSearchBackendEntry current = (UltraSearchBackendEntry) e1;
            UltraSearchBackendEntry next = (UltraSearchBackendEntry) e2;

            E.checkState(current == null || current.type().isVertex(),
                    "The current entry must be null or VERTEX");
            E.checkState(next != null && next.type().isEdge(),
                    "The next entry must be EDGE");

            if (current != null) {
                Id nextVertexId = IdGenerator.of(
                        next.<String>column(HugeKeys.OWNER_VERTEX));
                if (current.id().equals(nextVertexId)) {
                    current.subRow(next.row());
                    return current;
                }
            }

            return this.wrapByVertex(next);
        }

        private UltraSearchBackendEntry wrapByVertex(UltraSearchBackendEntry edge) {
            assert edge.type().isEdge();
            String ownerVertex = edge.column(HugeKeys.OWNER_VERTEX);
            E.checkState(ownerVertex != null, "Invalid backend entry");
            Id vertexId = IdGenerator.of(ownerVertex);
            UltraSearchBackendEntry vertex = new UltraSearchBackendEntry(HugeType.VERTEX,
                    vertexId);

            vertex.column(HugeKeys.ID, ownerVertex);
            vertex.column(HugeKeys.PROPERTIES, "");

            vertex.subRow(edge.row());
            return vertex;
        }

        public static String table(Directions direction) {
            assert direction == Directions.OUT || direction == Directions.IN;
            return TABLE_PREFIX + "_" + direction.string();
        }

        public static UltraSearchTable out(String store) {
            return new Edge(store, Directions.OUT);
        }

        public static UltraSearchTable in(String store) {
            return new Edge(store, Directions.IN);
        }
    }

    public abstract static class Index extends UltraSearchTableTemplate {

        public Index(String table) {
            super(table);
        }

        @Override
        protected BackendEntry mergeEntries(BackendEntry e1, BackendEntry e2) {
            UltraSearchBackendEntry current = (UltraSearchBackendEntry) e1;
            UltraSearchBackendEntry next = (UltraSearchBackendEntry) e2;

            E.checkState(current == null || current.type().isIndex(),
                    "The current entry must be null or INDEX");
            E.checkState(next != null && next.type().isIndex(),
                    "The next entry must be INDEX");

            if (current != null) {
                String currentId = this.entryId(current);
                String nextId = this.entryId(next);
                if (currentId.equals(nextId)) {
                    current.subRow(next.row());
                    return current;
                }
            }
            return next;
        }

        protected abstract String entryId(UltraSearchBackendEntry entry);
    }

    public static class SecondaryIndex extends Index {

        public static final String TABLE = "secondary_indexes";

        public SecondaryIndex(String store) {
            this(store, TABLE);
        }
        public SecondaryIndex(String store, String table) {
            super(joinTableName(store, table));

            this.define = new TableDefine();
            this.define.column(HugeKeys.FIELD_VALUES, VARCHAR);
            this.define.column(HugeKeys.INDEX_LABEL_ID, DATATYPE_IL);
            this.define.column(HugeKeys.ELEMENT_IDS, VARCHAR);
            this.define.keys(HugeKeys.FIELD_VALUES,
                    HugeKeys.INDEX_LABEL_ID,
                    HugeKeys.ELEMENT_IDS);
        }

        @Override
        protected final String entryId(UltraSearchBackendEntry entry) {
            String fieldValues = entry.column(HugeKeys.FIELD_VALUES);
            Integer labelId = entry.column(HugeKeys.INDEX_LABEL_ID);
            return SplicingIdGenerator.concat(fieldValues, labelId.toString());
        }
    }

    public static class SearchIndex extends SecondaryIndex {

        public static final String TABLE = "search_indexes";

        public SearchIndex(String store) {
            super(store, TABLE);
        }
    }

    public static class RangeIndex extends Index {

        public static final String TABLE = "range_indexes";

        public RangeIndex(String store) {
            super(joinTableName(store, TABLE));

            this.define = new TableDefine();
            this.define.column(HugeKeys.INDEX_LABEL_ID, DATATYPE_IL);
            this.define.column(HugeKeys.FIELD_VALUES, DOUBLE);
            this.define.column(HugeKeys.ELEMENT_IDS, VARCHAR);
            this.define.keys(HugeKeys.INDEX_LABEL_ID,
                    HugeKeys.FIELD_VALUES,
                    HugeKeys.ELEMENT_IDS);
        }

        @Override
        protected final String entryId(UltraSearchBackendEntry entry) {
            Double fieldValue = entry.<Double>column(HugeKeys.FIELD_VALUES);
            Integer labelId = entry.column(HugeKeys.INDEX_LABEL_ID);
            return SplicingIdGenerator.concat(labelId.toString(),
                    fieldValue.toString());
        }
    }
}
