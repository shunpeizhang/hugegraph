/*
 * Copyright 2017 HugeGraph Authors
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.baidu.hugegraph.backend.store.rocksdb;

import java.util.Map;

import org.apache.commons.io.FileUtils;

import com.baidu.hugegraph.backend.store.BackendMetrics;
import com.baidu.hugegraph.backend.store.rocksdb.RocksDBSessions.Session;
import com.baidu.hugegraph.util.Bytes;
import com.baidu.hugegraph.util.InsertionOrderUtil;

public class RocksDBMetrics implements BackendMetrics {

    private static final String INDEX_FILTER =
                                "rocksdb.estimate-table-readers-mem";
    private static final String MEM_TABLE = "rocksdb.cur-size-all-mem-tables";
    private static final String R_DATA_SIZE = "rocksdb.estimate-live-data-size";

    private final Session session;

    public RocksDBMetrics(Session session) {
        this.session = session;
    }

    @Override
    public Map<String, Object> getMetrics() {
        Map<String, Object> metrics = InsertionOrderUtil.newMap();
        // NOTE: the unit of rocksdb mem property is kb
        metrics.put(MEM_USED, this.getMemUsed() / Bytes.BASE);
        metrics.put(MEM_UNIT, "MB");
        String size = FileUtils.byteCountToDisplaySize(this.getDataSize());
        metrics.put(DATA_SIZE, size);
        return metrics;
    }

    private long getMemUsed() {
        long indexFilter = Long.parseLong(this.session.property(INDEX_FILTER));
        long memtable = Long.parseLong(this.session.property(MEM_TABLE));
        return indexFilter + memtable;
    }

    private long getDataSize() {
        return Long.parseLong(this.session.property(R_DATA_SIZE));
    }
}
