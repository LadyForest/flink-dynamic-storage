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

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.runtime.util.MemorySegmentPool;

import java.util.LinkedList;
import java.util.List;

/** MemorySegment pool from heap. */
public class HeapMemorySegmentPool implements MemorySegmentPool {

    private final LinkedList<MemorySegment> segments;
    private final int maxPages;
    private final int pageSize;

    private int numPage;

    public HeapMemorySegmentPool(int maxPages, int pageSize) {
        this.segments = new LinkedList<>();
        this.maxPages = maxPages;
        this.pageSize = pageSize;
        this.numPage = 0;
    }

    @Override
    public MemorySegment nextSegment() {
        if (this.segments.size() > 0) {
            return this.segments.poll();
        } else if (numPage < maxPages) {
            numPage++;
            return MemorySegmentFactory.allocateUnpooledSegment(pageSize);
        }

        return null;
    }

    @Override
    public int pageSize() {
        return pageSize;
    }

    @Override
    public void returnAll(List<MemorySegment> memory) {
        segments.addAll(memory);
    }

    @Override
    public int freePages() {
        return segments.size() + maxPages - numPage;
    }
}
