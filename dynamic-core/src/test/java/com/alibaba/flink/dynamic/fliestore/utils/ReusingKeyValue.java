/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.flink.dynamic.fliestore.utils;

import com.alibaba.flink.dynamic.fliestore.KeyValue;

import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.writer.BinaryRowWriter;

/**
 * Util class which reuses a {@link KeyValue} to test if other components handle reuse correctly.
 * This must be used along with {@link ReusingTestData}.
 */
public class ReusingKeyValue {

    private final BinaryRowData key;
    private final BinaryRowWriter keyWriter;
    private final BinaryRowData value;
    private final BinaryRowWriter valueWriter;
    private final KeyValue kv;

    public ReusingKeyValue() {
        this.key = new BinaryRowData(1);
        this.keyWriter = new BinaryRowWriter(key);
        this.value = new BinaryRowData(1);
        this.valueWriter = new BinaryRowWriter(value);
        this.kv = new KeyValue();
    }

    public KeyValue update(ReusingTestData data) {
        keyWriter.writeInt(0, data.key);
        keyWriter.complete();
        valueWriter.writeLong(0, data.value);
        valueWriter.complete();
        return kv.replace(key, data.sequenceNumber, data.valueKind, value);
    }
}
