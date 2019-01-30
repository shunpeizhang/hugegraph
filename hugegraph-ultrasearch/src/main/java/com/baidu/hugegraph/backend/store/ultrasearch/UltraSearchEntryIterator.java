package com.baidu.hugegraph.backend.store.ultrasearch;

import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.function.BiFunction;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.BackendEntryIterator;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.util.JsonUtil;
import com.baidu.hugegraph.util.E;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class UltraSearchEntryIterator extends BackendEntryIterator {

    private final JSONArray results;
    private final BiFunction<BackendEntry, BackendEntry, BackendEntry> merger;

    private BackendEntry next;
    private BackendEntry last;

    private Integer nextPos = -1;
    private Integer totalLen = -1;

    public UltraSearchEntryIterator(JSONArray rs, Query query,
                              BiFunction<BackendEntry, BackendEntry, BackendEntry> merger) {
        super(query);
        this.results = rs;
        this.merger = merger;
        this.next = null;
        this.last = null;

        if(null != results) totalLen = results.size();
        nextPos = 0;
    }

    @Override
    protected final boolean fetch() {
        if(null == this.results) return false;

        assert this.current == null;
        if (this.next != null) {
            this.current = this.next;
            this.next = null;
        }

        while (nextPos < totalLen) {
            UltraSearchBackendEntry e = this.row2Entry(this.results);
            this.last = e;
            BackendEntry merged = this.merger.apply(this.current, e);
            if (this.current == null) {
                // The first time to read
                this.current = merged;
            } else if (merged == this.current) {
                // Does the next entry belongs to the current entry
                assert merged != null;
            } else {
                // New entry
                assert this.next == null;
                this.next = merged;
                return true;
            }
        }

        return this.current != null;
    }

    @Override
    protected String pageState() {
        if (this.last == null) {
            return null;
        }
        if (this.fetched() <= this.query.limit() && this.next == null) {
            // There is no next page
            return null;
        }
        UltraSearchBackendEntry entry = (UltraSearchBackendEntry) this.last;
        PageState pageState = new PageState(entry.columnsMap());
        return pageState.toString();
    }

    @Override
    protected final long sizeOf(BackendEntry entry) {
        UltraSearchBackendEntry e = (UltraSearchBackendEntry) entry;
        int subRowsSize = e.subRows().size();
        return subRowsSize > 0 ? subRowsSize : 1L;
    }

    @Override
    protected final long offset() {
        return 0L;
    }

    @Override
    protected final long skip(BackendEntry entry, long skip) {
        UltraSearchBackendEntry e = (UltraSearchBackendEntry) entry;
        E.checkState(e.subRows().size() > skip, "Invalid entry to skip");
        for (long i = 0; i < skip; i++) {
            e.subRows().remove(0);
        }
        return e.subRows().size();
    }

    private UltraSearchBackendEntry row2Entry(JSONArray result) {
        HugeType type = this.query.resultType();
        UltraSearchBackendEntry entry = new UltraSearchBackendEntry(type);
        JSONObject fields = result.getJSONObject(nextPos).getJSONObject("fields");
        Iterator iter = fields.keys();
        while(iter.hasNext())
        {
            String name = (String)iter.next();
            Object value = fields.getJSONObject(name);
            entry.column(UltraSearchTable.parseKey(name), value);
        }
        ++nextPos;

        return entry;
    }

    @Override
    public void close() throws Exception {
        if(null != results) this.results.clear();
    }

    public static class PageState {

        private static final String CHARSET = "utf-8";
        private final Map<HugeKeys, Object> columns;

        public PageState(Map<HugeKeys, Object> columns) {
            this.columns = columns;
        }

        public Map<HugeKeys, Object> columns() {
            return this.columns;
        }

        @Override
        public String toString() {
            return Base64.getEncoder().encodeToString(this.toBytes());
        }

        public byte[] toBytes() {
            String json = JsonUtil.toJson(this.columns);
            try {
                return json.getBytes(CHARSET);
            } catch (UnsupportedEncodingException e) {
                throw new BackendException(e);
            }
        }

        public static PageState fromString(String page) {
            byte[] bytes;
            try {
                bytes = Base64.getDecoder().decode(page);
            } catch (Exception e) {
                throw new BackendException("Invalid page: '%s'", e, page);
            }
            return fromBytes(bytes);
        }

        private static PageState fromBytes(byte[] bytes) {
            String json;
            try {
                json = new String(bytes, CHARSET);
            } catch (UnsupportedEncodingException e) {
                throw new BackendException(e);
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> columns = JsonUtil.fromJson(json, Map.class);
            Map<HugeKeys, Object> keyColumns = new LinkedHashMap<>();
            for (Map.Entry<String, Object> entry : columns.entrySet()) {
                HugeKeys key = UltraSearchTable.parseKey(entry.getKey());
                keyColumns.put(key, entry.getValue());
            }
            return new PageState(keyColumns);
        }
    }
}
