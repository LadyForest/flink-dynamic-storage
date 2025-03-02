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
import com.alibaba.flink.dynamic.fliestore.utils.RecordReader;
import com.alibaba.flink.dynamic.fliestore.utils.ReusingTestData;
import com.alibaba.flink.dynamic.fliestore.utils.TestReusingRecordReader;

import org.apache.flink.table.runtime.generated.RecordComparator;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RecordReader}s which combines several other {@link RecordReader}s. */
public abstract class CombiningRecordReaderTestBase {

    protected static final RecordComparator KEY_COMPARATOR =
            (a, b) -> Integer.compare(a.getInt(0), b.getInt(0));

    protected abstract boolean addOnly();

    protected abstract List<ReusingTestData> getExpected(List<ReusingTestData> input);

    protected abstract RecordReader createRecordReader(List<TestReusingRecordReader> readers);

    @Test
    public void testRandom() throws IOException {
        for (int tries = 0; tries < 100; tries++) {
            runTest(generateRandomData());
        }
    }

    protected List<List<ReusingTestData>> parseData(String... stringsData) {
        List<List<ReusingTestData>> readersData = new ArrayList<>();
        for (String stringData : stringsData) {
            readersData.add(ReusingTestData.parse(stringData));
        }
        return readersData;
    }

    protected List<List<ReusingTestData>> generateRandomData() {
        Random random = new Random();
        int numReaders = random.nextInt(20) + 1;
        List<List<ReusingTestData>> readersData = new ArrayList<>();
        for (int i = 0; i < numReaders; i++) {
            readersData.add(
                    ReusingTestData.generateOrderedNoDuplicatedKeys(
                            random.nextInt(100) + 1, addOnly()));
        }
        return readersData;
    }

    protected void runTest(List<List<ReusingTestData>> readersData) throws IOException {
        Iterator<ReusingTestData> expectedIterator =
                getExpected(
                                readersData.stream()
                                        .flatMap(Collection::stream)
                                        .collect(Collectors.toList()))
                        .iterator();
        List<TestReusingRecordReader> readers = new ArrayList<>();
        for (List<ReusingTestData> readerData : readersData) {
            readers.add(new TestReusingRecordReader(readerData));
        }
        RecordReader recordReader = createRecordReader(readers);

        RecordReader.RecordIterator batch;
        while ((batch = recordReader.readBatch()) != null) {
            KeyValue kv;
            while ((kv = batch.next()) != null) {
                assertThat(expectedIterator.hasNext()).isTrue();
                ReusingTestData expected = expectedIterator.next();
                expected.assertEquals(kv);
            }
            batch.releaseBatch();
        }
        assertThat(expectedIterator.hasNext()).isFalse();
        recordReader.close();

        for (TestReusingRecordReader reader : readers) {
            reader.assertCleanUp();
        }
    }
}
