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

package org.apache.paimon.hive.mapred;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.hive.FileStoreTestUtils;
import org.apache.paimon.hive.RowDataContainer;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PaimonRecordReader}. */
public class PaimonRecordReaderTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testPk() throws Exception {
        Options conf = new Options();
        conf.set(CatalogOptions.WAREHOUSE, tempDir.toString());
        conf.set(CoreOptions.FILE_FORMAT, CoreOptions.FILE_FORMAT_AVRO);
        conf.set("bucket", "1");
        Table table =
                FileStoreTestUtils.createFileStoreTable(
                        conf,
                        RowType.of(
                                new DataType[] {DataTypes.BIGINT(), DataTypes.STRING()},
                                new String[] {"a", "b"}),
                        Collections.emptyList(),
                        Collections.singletonList("a"));

        StreamWriteBuilder streamWriteBuilder = table.newStreamWriteBuilder();
        StreamTableWrite write = streamWriteBuilder.newWrite();
        StreamTableCommit commit = streamWriteBuilder.newCommit();
        write.write(GenericRow.of(1L, BinaryString.fromString("Hi")));
        write.write(GenericRow.of(2L, BinaryString.fromString("Hello")));
        write.write(GenericRow.of(3L, BinaryString.fromString("World")));
        write.write(GenericRow.of(1L, BinaryString.fromString("Hi again")));
        write.write(GenericRow.ofKind(RowKind.DELETE, 2L, BinaryString.fromString("Hello")));
        commit.commit(0, write.prepareCommit(true, 0));

        write.close();
        commit.close();

        PaimonRecordReader reader = read(table, BinaryRow.EMPTY_ROW, 0);
        RowDataContainer container = reader.createValue();
        Set<String> actual = new HashSet<>();
        while (reader.next(null, container)) {
            InternalRow rowData = container.get();
            String value = rowData.getLong(0) + "|" + rowData.getString(1).toString();
            actual.add(value);
        }

        Set<String> expected = new HashSet<>();
        expected.add("1|Hi again");
        expected.add("3|World");
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testProjectionPushdown() throws Exception {
        Options conf = new Options();
        conf.set(CatalogOptions.WAREHOUSE, tempDir.toString());
        conf.set(CoreOptions.FILE_FORMAT, CoreOptions.FILE_FORMAT_AVRO);
        Table table =
                FileStoreTestUtils.createFileStoreTable(
                        conf,
                        RowType.of(
                                new DataType[] {
                                    DataTypes.INT(), DataTypes.BIGINT(), DataTypes.STRING()
                                },
                                new String[] {"a", "b", "c"}),
                        Collections.emptyList(),
                        Collections.emptyList());

        StreamWriteBuilder streamWriteBuilder = table.newStreamWriteBuilder();
        StreamTableWrite write = streamWriteBuilder.newWrite();
        StreamTableCommit commit = streamWriteBuilder.newCommit();
        write.write(GenericRow.of(1, 10L, BinaryString.fromString("Hi")));
        write.write(GenericRow.of(2, 20L, BinaryString.fromString("Hello")));
        write.write(GenericRow.of(1, 10L, BinaryString.fromString("Hi")));
        commit.commit(0, write.prepareCommit(true, 0));

        write.close();
        commit.close();

        PaimonRecordReader reader = read(table, BinaryRow.EMPTY_ROW, 0, Arrays.asList("c", "a"));
        RowDataContainer container = reader.createValue();
        Map<String, Integer> actual = new HashMap<>();
        while (reader.next(null, container)) {
            InternalRow rowData = container.get();
            String key = rowData.getInt(0) + "|" + rowData.getString(2).toString();
            actual.compute(key, (k, v) -> (v == null ? 0 : v) + 1);
        }

        Map<String, Integer> expected = new HashMap<>();
        expected.put("1|Hi", 2);
        expected.put("2|Hello", 1);
        assertThat(actual).isEqualTo(expected);
    }

    private PaimonRecordReader read(Table table, BinaryRow partition, int bucket) throws Exception {
        return read(table, partition, bucket, ((FileStoreTable) table).schema().fieldNames());
    }

    private PaimonRecordReader read(
            Table table, BinaryRow partition, int bucket, List<String> selectedColumns)
            throws Exception {
        for (Split split : table.newReadBuilder().newScan().plan().splits()) {
            DataSplit dataSplit = (DataSplit) split;
            if (dataSplit.partition().equals(partition) && dataSplit.bucket() == bucket) {
                List<String> originalColumns = ((FileStoreTable) table).schema().fieldNames();
                return new PaimonRecordReader(
                        table.newReadBuilder(),
                        new PaimonInputSplit(tempDir.toString(), dataSplit, (FileStoreTable) table),
                        originalColumns,
                        originalColumns,
                        selectedColumns,
                        null);
            }
        }
        throw new IllegalArgumentException(
                "Input split not found for partition " + partition + " and bucket " + bucket);
    }

    @Test
    public void testProgressTracking() throws Exception {
        Options conf = new Options();
        conf.set(CatalogOptions.WAREHOUSE, tempDir.toString());
        conf.set(CoreOptions.FILE_FORMAT, CoreOptions.FILE_FORMAT_AVRO);
        conf.set("bucket", "1");
        Table table =
                FileStoreTestUtils.createFileStoreTable(
                        conf,
                        RowType.of(
                                new DataType[] {DataTypes.BIGINT(), DataTypes.STRING()},
                                new String[] {"a", "b"}),
                        Collections.emptyList(),
                        Collections.emptyList());

        StreamWriteBuilder streamWriteBuilder = table.newStreamWriteBuilder();
        StreamTableWrite write = streamWriteBuilder.newWrite();
        StreamTableCommit commit = streamWriteBuilder.newCommit();

        // Write multiple batches to create multiple files
        for (int batch = 0; batch < 3; batch++) {
            for (int i = 0; i < 10; i++) {
                write.write(
                        GenericRow.of(
                                (long) (batch * 10 + i),
                                BinaryString.fromString("value_" + (batch * 10 + i))));
            }
            commit.commit(batch, write.prepareCommit(true, batch));
        }

        write.close();
        commit.close();

        PaimonRecordReader reader = read(table, BinaryRow.EMPTY_ROW, 0);
        RowDataContainer container = reader.createValue();

        // Check initial progress
        assertThat(reader.getProgress()).isEqualTo(0.0f);

        int recordCount = 0;
        float previousProgress = 0.0f;

        // Read all records and verify progress changes and increases monotonically
        List<Float> progressValues = new ArrayList<>();
        while (reader.next(null, container)) {
            recordCount++;
            float currentProgress = reader.getProgress();
            progressValues.add(currentProgress);

            // Progress should increase or stay the same
            assertThat(currentProgress).isGreaterThanOrEqualTo(previousProgress);
            // Progress should be between 0 and 1
            assertThat(currentProgress).isBetween(0.0f, 1.0f);

            previousProgress = currentProgress;
        }

        // After reading all records, progress should be 1.0
        assertThat(reader.getProgress()).isEqualTo(1.0f);
        assertThat(recordCount).isEqualTo(30); // 3 batches * 10 records each

        // Verify that progress actually changes during reading (not just 0 and 1)
        // With 30 records, we should see multiple different progress values
        long distinctProgressCount = progressValues.stream().distinct().count();
        assertThat(distinctProgressCount).isGreaterThan(2);

        // Verify approximate accuracy: progress should be roughly proportional to records read
        // At 50% of records, progress should be around 0.5 (with some tolerance)
        int midPoint = recordCount / 2;
        if (midPoint < progressValues.size()) {
            float midProgress = progressValues.get(midPoint);
            // Progress at midpoint should be around 0.5 (±0.2 tolerance)
            assertThat(midProgress).isBetween(0.3f, 0.7f);
        }

        // Verify early progress: after reading 3 records (~10%), progress should be > 0
        if (progressValues.size() > 3) {
            float earlyProgress = progressValues.get(2);
            assertThat(earlyProgress).isGreaterThan(0.0f);
        }

        // Verify late progress: before finishing, progress should be close to 1.0
        if (progressValues.size() > 3) {
            float lateProgress = progressValues.get(progressValues.size() - 3);
            assertThat(lateProgress).isLessThan(1.0f);
            assertThat(lateProgress).isGreaterThan(0.5f);
        }
    }

    @Test
    public void testProgressTrackingWithProjection() throws Exception {
        Options conf = new Options();
        conf.set(CatalogOptions.WAREHOUSE, tempDir.toString());
        conf.set(CoreOptions.FILE_FORMAT, CoreOptions.FILE_FORMAT_AVRO);
        Table table =
                FileStoreTestUtils.createFileStoreTable(
                        conf,
                        RowType.of(
                                new DataType[] {
                                    DataTypes.INT(), DataTypes.BIGINT(), DataTypes.STRING()
                                },
                                new String[] {"a", "b", "c"}),
                        Collections.emptyList(),
                        Collections.emptyList());

        StreamWriteBuilder streamWriteBuilder = table.newStreamWriteBuilder();
        StreamTableWrite write = streamWriteBuilder.newWrite();
        StreamTableCommit commit = streamWriteBuilder.newCommit();

        // Write 20 records
        for (int i = 0; i < 20; i++) {
            write.write(GenericRow.of(i, (long) i * 10, BinaryString.fromString("value_" + i)));
        }
        commit.commit(0, write.prepareCommit(true, 0));

        write.close();
        commit.close();

        // Read with projection
        PaimonRecordReader reader = read(table, BinaryRow.EMPTY_ROW, 0, Arrays.asList("c", "a"));
        RowDataContainer container = reader.createValue();

        // Check initial progress
        assertThat(reader.getProgress()).isEqualTo(0.0f);

        int recordCount = 0;
        float previousProgress = 0.0f;

        // Read all records and verify progress changes and increases monotonically
        List<Float> progressValues = new ArrayList<>();
        while (reader.next(null, container)) {
            recordCount++;
            float currentProgress = reader.getProgress();
            progressValues.add(currentProgress);

            assertThat(currentProgress).isGreaterThanOrEqualTo(previousProgress);
            assertThat(currentProgress).isBetween(0.0f, 1.0f);

            previousProgress = currentProgress;
        }

        assertThat(reader.getProgress()).isEqualTo(1.0f);
        assertThat(recordCount).isEqualTo(20);

        // Verify that progress actually changes during reading
        long distinctProgressCount = progressValues.stream().distinct().count();
        assertThat(distinctProgressCount).isGreaterThan(2);

        // Verify approximate accuracy at different points
        // At 25% of records, progress should be around 0.25
        if (progressValues.size() > 5) {
            float quarterProgress = progressValues.get(4);
            assertThat(quarterProgress).isBetween(0.1f, 0.6f);
        }

        // At 75% of records, progress should be around 0.75
        if (progressValues.size() > 15) {
            float threeQuarterProgress = progressValues.get(14);
            assertThat(threeQuarterProgress).isBetween(0.4f, 0.9f);
        }
    }
}
