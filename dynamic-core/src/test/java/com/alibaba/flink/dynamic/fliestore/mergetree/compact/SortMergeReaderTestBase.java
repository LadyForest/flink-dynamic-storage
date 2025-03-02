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
import java.util.ArrayList;
import java.util.List;

/** Tests for {@link SortMergeReader}. */
public abstract class SortMergeReaderTestBase extends CombiningRecordReaderTestBase {

    protected abstract Accumulator createAccumulator();

    @Override
    protected RecordReader createRecordReader(List<TestReusingRecordReader> readers) {
        return new SortMergeReader(new ArrayList<>(readers), KEY_COMPARATOR, createAccumulator());
    }

    @Test
    public void testEmpty() throws IOException {
        runTest(parseData(""));
        runTest(parseData("", "", ""));
    }

    @Test
    public void testAlternateKeys() throws IOException {
        runTest(
                parseData(
                        "1, 1, +, 100 | 3, 2, +, 300 | 5, 3, +, 200 | 7, 4, +, 600 | 9, 20, +, 400",
                        "0, 5, +, 0",
                        "0, 10, +, 0",
                        "",
                        "2, 6, +, 200 | 4, 7, +, 400 | 6, 8, +, 600 | 8, 9, +, 800"));
    }

    @Test
    public void testDuplicateKeys() throws IOException {
        runTest(parseData("1, 1, +, 100 | 3, 3, +, 300", "1, 4, +, 200 | 3, 5, +, 300"));
    }

    @Test
    public void testLongTailRecords() throws IOException {
        runTest(
                parseData(
                        "1, 1, +, 100 | 2, 500, +, 200",
                        "1, 3, +, 100 | 3, 4, +, 300 | 5, 501, +, 500 | 7, 503, +, 700 | "
                                + "8, 504, +, 800 | 9, 505, +, 900 | 10, 506, +, 1000 | "
                                + "11, 507, +, 1100 | 12, 508, +, 1200 | 13, 509, +, 1300"));
    }

    /** Tests for {@link SortMergeReader} with {@link DeduplicateAccumulator}. */
    public static class WithDeduplicateAccumulator extends SortMergeReaderTestBase {

        @Override
        protected boolean addOnly() {
            return false;
        }

        @Override
        protected List<ReusingTestData> getExpected(List<ReusingTestData> input) {
            return AccumulatorTestUtils.getExpectedForDeduplicate(input);
        }

        @Override
        protected Accumulator createAccumulator() {
            return new DeduplicateAccumulator();
        }
    }

    /** Tests for {@link SortMergeReader} with {@link ValueCountAccumulator}. */
    public static class WithValueRecordAccumulatorTest extends SortMergeReaderTestBase {

        @Override
        protected boolean addOnly() {
            return true;
        }

        @Override
        protected List<ReusingTestData> getExpected(List<ReusingTestData> input) {
            return AccumulatorTestUtils.getExpectedForValueCount(input);
        }

        @Override
        protected Accumulator createAccumulator() {
            return new ValueCountAccumulator();
        }

        @Test
        public void testCancelingRecords() throws IOException {
            runTest(
                    parseData(
                            "1, 1, +, 100 | 3, 5, +, -300 | 5, 300, +, 300",
                            "",
                            "1, 4, +, -200 | 3, 3, +, 300",
                            "5, 100, +, -200 | 7, 123, +, -500",
                            "7, 321, +, 200",
                            "7, 456, +, 300"));
            runTest(parseData("1, 2, +, 100", "1, 1, +, -100"));
        }
    }
}
