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

package com.alibaba.flink.dynamic.fliestore.mergetree;

import com.alibaba.flink.dynamic.fliestore.KeyValue;
import com.alibaba.flink.dynamic.fliestore.ValueKind;
import com.alibaba.flink.dynamic.fliestore.mergetree.compact.Accumulator;

import org.apache.flink.table.data.RowData;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;

/**
 * Append only memory table for storing key-values. When it is full, it will be flushed to disk and
 * form an SST file.
 */
public interface MemTable {

    /**
     * Put a record with sequence number and value kind.
     *
     * @return True, if the record was successfully written, false, if the mem table was full.
     */
    boolean put(long sequenceNumber, ValueKind valueKind, RowData key, RowData value)
            throws IOException;

    /** Record size of this table. */
    int size();

    /**
     * Returns an iterator over the records in this table. The elements are returned in the order of
     * key and sequence number and elements with the same key will be merged by the given {@link
     * Accumulator}.
     */
    Iterator<KeyValue> iterator(Comparator<RowData> keyComparator, Accumulator accumulator);

    /** Removes all records from this table. The table will be empty after this call returns. */
    void clear();
}
