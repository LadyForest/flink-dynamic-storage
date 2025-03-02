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
import com.alibaba.flink.dynamic.fliestore.utils.ReusingTestData;
import com.alibaba.flink.dynamic.fliestore.utils.TestReusingRecordReader;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/** Tests for {@link ConcatRecordReader}. */
public class ConcatRecordReaderTest extends CombiningRecordReaderTestBase {

    @Override
    protected boolean addOnly() {
        return false;
    }

    @Override
    protected List<ReusingTestData> getExpected(List<ReusingTestData> input) {
        return input;
    }

    @Override
    protected RecordReader createRecordReader(List<TestReusingRecordReader> readers) {
        return new ConcatRecordReader(
                readers.stream()
                        .map(r -> (Supplier<RecordReader>) () -> r)
                        .collect(Collectors.toList()));
    }

    @Test
    public void testSmallData() throws IOException {
        runTest(
                parseData(
                        "1, 1, +, 100 | 3, 2, +, 300 | 5, 3, -, 500 | "
                                + "7, 4, +, 700 | 9, 20, +, 900",
                        "",
                        "12, 6, +, 1200 |  14, 7, +, 1400 |  16, 8, -, 1600 |  18, 9, -, 1800"));
        runTest(
                parseData(
                        " 1, 10, +, 100 |  3, 20, +, 300 |  5, 30, -, 500 | "
                                + " 7, 40, +, 700 |  9, 200, -, 900",
                        "",
                        " 12, 60, +, 1200 |  14, 70, -, 1400 |  16, 80, +, 1600 |  18, 90, -, 1800"));
    }
}
