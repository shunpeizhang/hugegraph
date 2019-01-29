package com.baidu.hugegraph.backend.store.ultrasearch;

import com.baidu.hugegraph.backend.store.BackendFeatures;

public class UltraSearchFeatures implements BackendFeatures {

    @Override
    public boolean supportsScanToken() {
        return false;
    }

    @Override
    public boolean supportsScanKeyPrefix() {
        return false;
    }

    @Override
    public boolean supportsScanKeyRange() {
        return false;
    }

    @Override
    public boolean supportsQuerySchemaByName() {
        // MySQL support secondary index
        return true;
    }

    @Override
    public boolean supportsQueryByLabel() {
        // MySQL support secondary index
        return true;
    }

    @Override
    public boolean supportsQueryWithRangeCondition() {
        return true;
    }

    @Override
    public boolean supportsQueryWithOrderBy() {
        return true;
    }

    @Override
    public boolean supportsQueryWithContains() {
        return false;
    }

    @Override
    public boolean supportsQueryWithContainsKey() {
        return false;
    }

    @Override
    public boolean supportsQueryByPage() {
        return true;
    }

    @Override
    public boolean supportsDeleteEdgeByLabel() {
        return true;
    }

    @Override
    public boolean supportsUpdateVertexProperty() {
        return false;
    }

    @Override
    public boolean supportsMergeVertexProperty() {
        return false;
    }

    @Override
    public boolean supportsUpdateEdgeProperty() {
        return false;
    }

    @Override
    public boolean supportsTransaction() {
        // MySQL support tx
        return true;
    }

    @Override
    public boolean supportsNumberType() {
        return true;
    }
}
