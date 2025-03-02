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

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A testing {@link RecordReader} using {@link ReusingTestData} which produces batches of random
 * sizes (possibly empty). {@link KeyValue}s produced by the same reader is reused to test that
 * other components correctly handles the reusing.
 */
public class TestReusingRecordReader implements RecordReader {

    private final List<ReusingTestData> testData;
    private final ReusingKeyValue reuse;

    private final List<TestRecordIterator> producedBatches;
    private final Random random;

    private int nextLowerBound;
    private boolean closed;

    public TestReusingRecordReader(List<ReusingTestData> testData) {
        this.testData = testData;
        this.reuse = new ReusingKeyValue();

        this.producedBatches = new ArrayList<>();
        this.random = new Random();

        this.nextLowerBound = 0;
        this.closed = false;
    }

    @Nullable
    @Override
    public RecordIterator readBatch() {
        assertThat(nextLowerBound != -1).isTrue();
        if (nextLowerBound == testData.size() && random.nextBoolean()) {
            nextLowerBound = -1;
            return null;
        }
        int upperBound = random.nextInt(testData.size() - nextLowerBound + 1) + nextLowerBound;
        TestRecordIterator iterator = new TestRecordIterator(nextLowerBound, upperBound);
        nextLowerBound = upperBound;
        producedBatches.add(iterator);
        return iterator;
    }

    @Override
    public void close() throws IOException {
        closed = true;
    }

    public void assertCleanUp() {
        assertThat(closed).isTrue();
        for (TestRecordIterator iterator : producedBatches) {
            assertThat(iterator.released).isTrue();
        }
    }

    private class TestRecordIterator implements RecordIterator {

        private final int upperBound;

        private int next;
        private boolean released;

        private TestRecordIterator(int lowerBound, int upperBound) {
            this.upperBound = upperBound;

            this.next = lowerBound;
            this.released = false;
        }

        @Override
        public KeyValue next() throws IOException {
            assertThat(next != -1).isTrue();
            if (next == upperBound) {
                next = -1;
                return null;
            }
            KeyValue result = reuse.update(testData.get(next));
            next++;
            return result;
        }

        @Override
        public void releaseBatch() {
            this.released = true;
        }
    }
}
