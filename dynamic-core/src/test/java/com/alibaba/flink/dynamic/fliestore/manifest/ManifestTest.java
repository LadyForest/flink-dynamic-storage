/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.flink.dynamic.fliestore.manifest;

import com.alibaba.flink.dynamic.fliestore.DefaultFileFormat;
import com.alibaba.flink.dynamic.fliestore.FileFormat;
import com.alibaba.flink.dynamic.fliestore.TestKeyValueGenerator;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ManifestFile} and {@link ManifestList}. */
public class ManifestTest {

    private final ManifestTestDataGenerator gen;

    private final FileFormat avro =
            DefaultFileFormat.fromIdentifier(
                    ManifestTest.class.getClassLoader(), "avro", new Configuration());

    public ManifestTest() {
        this.gen = new ManifestTestDataGenerator();
    }

    @Test
    public void testWriteAndReadManifestFile() throws IOException {
        List<ManifestEntry> entries = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            entries.add(gen.next());
        }
        ManifestFileMeta meta = gen.createManifestFileMeta(entries);
        Path path = new Path(System.getProperty("java.io.tmpdir") + "/" + meta.fileName());
        ManifestFile manifestFile =
                new ManifestFile(
                        new Configuration(),
                        TestKeyValueGenerator.PARTITION_TYPE,
                        TestKeyValueGenerator.ROW_TYPE,
                        TestKeyValueGenerator.KEY_TYPE,
                        avro);

        ManifestFileMeta actualMeta = manifestFile.write(entries, path);
        // we do not check file size as we can't know in advance
        checkMetaIgnoringFileSize(meta, actualMeta);
        List<ManifestEntry> actualEntries = manifestFile.read(path);
        assertThat(actualEntries).isEqualTo(entries);
    }

    private void checkMetaIgnoringFileSize(ManifestFileMeta expected, ManifestFileMeta actual) {
        assertThat(actual.fileName()).isEqualTo(expected.fileName());
        assertThat(actual.numAddedFiles()).isEqualTo(expected.numAddedFiles());
        assertThat(actual.numDeletedFiles()).isEqualTo(expected.numDeletedFiles());
        assertThat(actual.partitionStats()).isEqualTo(expected.partitionStats());
    }

    @Test
    public void testWriteAndReadManifestList() throws IOException {
        Random random = new Random();
        List<ManifestFileMeta> metas = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            List<ManifestEntry> entries = new ArrayList<>();
            for (int j = random.nextInt(10) + 1; j > 0; j--) {
                entries.add(gen.next());
            }
            metas.add(gen.createManifestFileMeta(entries));
        }
        Path path =
                new Path(
                        System.getProperty("java.io.tmpdir")
                                + "/manifest-list-"
                                + UUID.randomUUID());
        ManifestList manifestList =
                new ManifestList(new Configuration(), TestKeyValueGenerator.PARTITION_TYPE, avro);

        manifestList.write(metas, path);
        List<ManifestFileMeta> actualMetas = manifestList.read(path);
        assertThat(actualMetas).isEqualTo(metas);
    }
}
