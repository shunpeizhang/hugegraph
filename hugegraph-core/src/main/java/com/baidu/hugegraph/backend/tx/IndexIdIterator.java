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

package com.baidu.hugegraph.backend.tx;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Supplier;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.ConditionQuery;

public class IndexIdIterator implements Iterator<Id> {

    private final ConditionQuery query;
    private final Supplier<PaginatedIds> idsFetcher;

    // 这个page代表的是索引表中的Id（不是顶点和边的Id）编码后的值
    // 这两个参数是不是可以直接用PaginatedIds表示
    private String page;
    private List<Id> results;
    // Can be change to pageSize
    private long limit;
    private int cursor;
    private boolean finished;

    public IndexIdIterator(ConditionQuery query, long limit,
                           Supplier<PaginatedIds> idsFetcher) {
        this.query = query;
        this.idsFetcher = idsFetcher;
        this.limit = limit;

        this.page = "";
        this.results = null;
        this.cursor = 0;
        this.finished = false;
    }

    public String page() {
        return this.page;
    }

    public void page(String page) {
        this.page = page;
    }

    public long limit() {
        return this.limit;
    }

    public void limit(long limit) {
        this.limit = limit;
    }

    @Override
    public boolean hasNext() {
        if (this.results == null || this.cursor >= this.results.size()) {
            this.fetch();
        }
        assert this.results != null;
        return this.cursor < this.results.size();
    }

    private void fetch() {
        if (this.finished) {
            return;
        }

        this.query.page(this.page);
        this.query.limit(this.limit);
        PaginatedIds paginatedIds = this.idsFetcher.get();
        this.results = paginatedIds.ids();
        this.page = paginatedIds.page();
        this.cursor = 0;

        System.out.println(">> Next page: " + page);

        if (this.results.size() != this.limit || this.page == null) {
            this.finished = true;
        }
    }

    @Override
    public Id next() {
        if (!this.hasNext()) {
            throw new NoSuchElementException();
        }
        return this.results.get(this.cursor++);
    }
}
