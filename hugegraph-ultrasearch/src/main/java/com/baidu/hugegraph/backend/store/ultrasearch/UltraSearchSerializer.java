package com.baidu.hugegraph.backend.store.ultrasearch;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.backend.serializer.TableBackendEntry;
import com.baidu.hugegraph.backend.serializer.TableSerializer;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.schema.SchemaElement;
import com.baidu.hugegraph.structure.HugeElement;
import com.baidu.hugegraph.structure.HugeProperty;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.util.InsertionOrderUtil;
import com.baidu.hugegraph.util.JsonUtil;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class UltraSearchSerializer extends TableSerializer {

    @Override
    public UltraSearchBackendEntry newBackendEntry(HugeType type, Id id) {
        return new UltraSearchBackendEntry(type, id);
    }

    @Override
    protected TableBackendEntry newBackendEntry(TableBackendEntry.Row row) {
        return new UltraSearchBackendEntry(row);
    }

    @Override
    protected UltraSearchBackendEntry convertEntry(BackendEntry backendEntry) {
        if (!(backendEntry instanceof UltraSearchBackendEntry)) {
            throw new BackendException("Not supported by UltraSearchSerializer");
        }
        return (UltraSearchBackendEntry) backendEntry;
    }

    @Override
    protected Set<String> parseIndexElemIds(TableBackendEntry entry) {
        Set<String> elemIds = InsertionOrderUtil.newSet();
        elemIds.add(entry.column(HugeKeys.ELEMENT_IDS));
        for (TableBackendEntry.Row row : entry.subRows()) {
            elemIds.add(row.column(HugeKeys.ELEMENT_IDS));
        }
        return elemIds;
    }

    @Override
    protected Id toId(Number number) {
        return IdGenerator.of(number.longValue());
    }

    @Override
    protected Id[] toIdArray(Object object) {
        assert object instanceof String;
        //String value = (String) object;
        //Number[] values = JsonUtil.fromJson(value, Number[].class);
        JSONArray value = (JSONArray) object;

        //if(0 == values.length) return null;

        Id[] ids = new Id[value.size()];
        for (int iPos = 0;  value.size() > iPos; ++iPos) {
            ids[iPos] = IdGenerator.of(value.getLong(iPos));
        }
        return ids;
    }

    @Override
    protected Object toLongSet(Collection<Id> ids) {
        //if(0 == ids.size()) return null;

        return this.toLongList(ids);
    }

    @Override
    protected Object toLongList(Collection<Id> ids) {
        //if(0 == ids.size()) return null;

        long[] values = new long[ids.size()];
        int i = 0;
        for (Id id : ids) {
            values[i++] = id.asLong();
        }
        return JsonUtil.toJson(values);
    }

    @Override
    protected void formatProperty(HugeProperty<?> prop,
                                  TableBackendEntry.Row row) {
        throw new BackendException("Not support updating single property " +
                "by MySQL");
    }

    @Override
    protected void formatProperties(HugeElement element,
                                    TableBackendEntry.Row row) {
        Map<Number, Object> properties = new HashMap<>();
        // Add all properties of a Vertex
        for (HugeProperty<?> prop : element.getProperties().values()) {
            Number key = prop.propertyKey().id().asLong();
            Object val = prop.value();
            properties.put(key, val);
        }

        //if(0 < properties.size())
            row.column(HugeKeys.PROPERTIES, JsonUtil.toJson(properties));
    }

    @Override
    protected void parseProperties(HugeElement element,
                                   TableBackendEntry.Row row) {
        JSONObject properties = row.column(HugeKeys.PROPERTIES);
        // Query edge will wraped by a vertex, whose properties is empty
        if (properties.isEmpty()) {
            return;
        }

        @SuppressWarnings("unchecked")
        Map<String, Object> props = JsonUtil.fromJson(properties.toString(), Map.class);
        for (Map.Entry<String, Object> prop : props.entrySet()) {
            /*
             * The key is string instead of int, because the key in json
             * must be string
             */
            Id pkeyId = this.toId(Long.valueOf(prop.getKey()));
            String colJson = JsonUtil.toJson(prop.getValue());
            this.parseProperty(pkeyId, colJson, element);
        }
    }

    @Override
    protected void writeUserdata(SchemaElement schema,
                                 TableBackendEntry entry) {
        assert entry instanceof UltraSearchBackendEntry;

        //if(0 < schema.userdata().size())
            entry.column(HugeKeys.USER_DATA, JsonUtil.toJson(schema.userdata()));
    }

    @Override
    protected void readUserdata(SchemaElement schema,
                                TableBackendEntry entry) {
        assert entry instanceof UltraSearchBackendEntry;
        // Parse all user data of a schema element
        JSONObject json = entry.column(HugeKeys.USER_DATA);
        @SuppressWarnings("unchecked")
        Map<String, Object> userdata = JsonUtil.fromJson(json.toString(), Map.class);
        for (Map.Entry<String, Object> e : userdata.entrySet()) {
            schema.userdata(e.getKey(), e.getValue());
        }
    }
}
