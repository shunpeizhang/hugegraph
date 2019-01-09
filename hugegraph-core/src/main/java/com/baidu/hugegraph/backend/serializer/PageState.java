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

package com.baidu.hugegraph.backend.serializer;

import java.util.Base64;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.util.Bytes;
import com.baidu.hugegraph.util.E;

public class PageState {

    private final byte[] position;
    // TODO: seems no use
    private final int offset;

    public PageState(byte[] position, int offset) {
        E.checkNotNull(position, "position");
        this.position = position;
        this.offset = offset;
    }

    public byte[] position() {
        return this.position;
    }

    public int offset() {
        return this.offset;
    }

    @Override
    public String toString() {
        return Base64.getEncoder().encodeToString(this.toBytes());
    }

    public byte[] toBytes() {
        int length = 2 + this.position.length + BytesBuffer.INT_LEN;
        BytesBuffer buffer = BytesBuffer.allocate(length);
        buffer.writeBytes(this.position);
        buffer.writeInt(this.offset);
        return buffer.bytes();
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

    public static PageState fromBytes(byte[] bytes) {
        if (bytes.length == 0) {
            // The first page
            return new PageState(new byte[0], 0);
        }
        try {
            BytesBuffer buffer = BytesBuffer.wrap(bytes);
            return new PageState(buffer.readBytes(), buffer.readInt());
        } catch (Exception e) {
            throw new BackendException("Invalid page: '0x%s'",
                                       e, Bytes.toHex(bytes));
        }
    }
}
