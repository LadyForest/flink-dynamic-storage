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

import org.apache.flink.table.data.RowData;

import javax.annotation.Nullable;

/**
 * A {@link Accumulator} where key is primary key (unique) and value is the full record, only keep
 * the latest one.
 */
public class DeduplicateAccumulator implements Accumulator {

    private RowData latestValue;

    @Override
    public void reset() {
        latestValue = null;
    }

    @Override
    public void add(RowData value) {
        latestValue = value;
    }

    @Override
    @Nullable
    public RowData getValue() {
        return latestValue;
    }

    @Override
    public Accumulator copy() {
        return new DeduplicateAccumulator();
    }
}
