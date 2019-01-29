package com.baidu.hugegraph.backend.store.ultrasearch;

import java.util.Collection;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.serializer.TableBackendEntry;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.type.HugeType;

public class UltraSearchBackendEntry extends TableBackendEntry {

    public UltraSearchBackendEntry(Id id) {
        super(id);
    }

    public UltraSearchBackendEntry(HugeType type) {
        this(type, null);
    }

    public UltraSearchBackendEntry(HugeType type, Id id) {
        this(new Row(type, id));
    }

    public UltraSearchBackendEntry(TableBackendEntry.Row row) {
        super(row);
    }

    @Override
    public String toString() {
        return String.format("UltraSearchBackendEntry{%s, sub-rows: %s}",
                this.row().toString(),
                this.subRows().toString());
    }

    @Override
    public int columnsSize() {
        throw new RuntimeException("Not supported by MySQL");
    }

    @Override
    public Collection<BackendColumn> columns() {
        throw new RuntimeException("Not supported by MySQL");
    }

    @Override
    public void columns(Collection<BackendColumn> bytesColumns) {
        throw new RuntimeException("Not supported by MySQL");
    }

    @Override
    public void columns(BackendColumn... bytesColumns) {
        throw new RuntimeException("Not supported by MySQL");
    }

    @Override
    public void merge(BackendEntry other) {
        throw new RuntimeException("Not supported by MySQL");
    }

    @Override
    public void clear() {
        throw new RuntimeException("Not supported by MySQL");
    }
}



