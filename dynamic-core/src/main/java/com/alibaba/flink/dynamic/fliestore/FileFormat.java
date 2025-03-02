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

package com.alibaba.flink.dynamic.fliestore;

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayList;
import java.util.List;

/** Factory class which creates reader and writer factories for specific file format. */
public interface FileFormat {

    /**
     * Create a {@link BulkFormat} from the type.
     *
     * @param filters A list of filters in conjunctive form for filtering on a best-effort basis.
     */
    BulkFormat<RowData, FileSourceSplit> createReaderFactory(
            RowType type, List<ResolvedExpression> filters);

    /** Create a {@link BulkWriter.Factory} from the type. */
    BulkWriter.Factory<RowData> createWriterFactory(RowType type);

    default BulkFormat<RowData, FileSourceSplit> createReaderFactory(RowType rowType) {
        return createReaderFactory(rowType, new ArrayList<>());
    }
}
