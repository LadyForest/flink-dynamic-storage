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

import com.alibaba.flink.dynamic.fliestore.KeyValue;

import org.apache.flink.table.data.RowData;

import javax.annotation.Nullable;

import java.io.Serializable;

/** Accumulators to merge multiple {@link KeyValue}s. */
public interface Accumulator extends Serializable {

    /** Reset the accumulator to its default state. */
    void reset();

    /** Add the given {@link RowData} to the accumulator. */
    void add(RowData value);

    /** Get current accumulated value. Return null if this accumulated result should be skipped. */
    @Nullable
    RowData getValue();

    /** Create a new accumulator object with the same functionality as this one. */
    Accumulator copy();
}
