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

package com.alibaba.flink.dynamic.fliestore.mergetree.compact;

import com.alibaba.flink.dynamic.fliestore.utils.RecordReader;

import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.function.Supplier;

/**
 * This reader is to concatenate a list of {@link RecordReader}s and read them sequentially. The
 * input list is already sorted by key and sequence number, and the key intervals do not overlap
 * each other.
 */
public class ConcatRecordReader implements RecordReader {

    private final Queue<Supplier<RecordReader>> queue;

    private RecordReader current;

    public ConcatRecordReader(List<Supplier<RecordReader>> sortedReaderSuppliers) {
        sortedReaderSuppliers.forEach(
                supplier ->
                        Preconditions.checkNotNull(supplier, "Reader supplier must not be null."));
        this.queue = new LinkedList<>(sortedReaderSuppliers);
    }

    @Nullable
    @Override
    public RecordIterator readBatch() throws IOException {
        while (true) {
            if (current != null) {
                RecordIterator iterator = current.readBatch();
                if (iterator != null) {
                    return iterator;
                }
                current.close();
                current = null;
            } else if (queue.size() > 0) {
                current = queue.poll().get();
            } else {
                return null;
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (current != null) {
            current.close();
        }
    }
}
