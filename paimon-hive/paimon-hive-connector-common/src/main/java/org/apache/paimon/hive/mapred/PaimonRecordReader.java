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

import org.apache.paimon.KeyValue;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.JoinedRow;
import org.apache.paimon.hive.RowDataContainer;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.utils.ProjectedRow;

import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.paimon.hive.utils.HiveUtils.createPredicate;
import static org.apache.paimon.hive.utils.HiveUtils.extractTagName;

/**
 * Base {@link RecordReader} for paimon. Reads {@link KeyValue}s from data files and picks out
 * {@link InternalRow} for Hive to consume.
 *
 * <p>NOTE: To support projection push down, when {@code selectedColumns} does not match {@code
 * columnNames} this reader will still produce records of the original schema. However, columns not
 * in {@code selectedColumns} will be null.
 */
public class PaimonRecordReader implements RecordReader<Void, RowDataContainer> {

    private final RecordReaderIterator<InternalRow> iterator;
    private final long splitLength;

    // project from selectedColumns to hiveColumns
    @Nullable private final ProjectedRow reusedProjectedRow;
    @Nullable private final JoinedRow addTagToPartFieldRow;

    private float progress;

    // Fields for precise progress tracking
    private final long totalRowCount;
    private long completedRows;
    private final boolean supportsRowCountTracking;

    /**
     * @param paimonColumns columns stored in Paimon table
     * @param hiveColumns columns Hive expects
     * @param selectedColumns columns we really have to read
     */
    public PaimonRecordReader(
            ReadBuilder readBuilder,
            PaimonInputSplit split,
            List<String> paimonColumns,
            List<String> hiveColumns,
            List<String> selectedColumns,
            @Nullable String tagToPartField)
            throws IOException {

        LinkedHashMap<String, Integer> paimonColumnIndexMap =
                IntStream.range(0, paimonColumns.size())
                        .boxed()
                        .collect(
                                Collectors.toMap(
                                        index -> paimonColumns.get(index).toLowerCase(),
                                        index -> index,
                                        (existing, replacement) -> existing,
                                        LinkedHashMap::new));

        if (!new ArrayList<>(paimonColumnIndexMap.keySet()).equals(selectedColumns)) {
            readBuilder.withProjection(
                    selectedColumns.stream().mapToInt(paimonColumnIndexMap::get).toArray());
        }

        if (hiveColumns.equals(selectedColumns)) {
            reusedProjectedRow = null;
        } else {
            reusedProjectedRow =
                    ProjectedRow.from(
                            hiveColumns.stream().mapToInt(selectedColumns::indexOf).toArray());
        }

        this.iterator =
                new RecordReaderIterator<>(readBuilder.newRead().createReader(split.split()));
        this.splitLength = split.getLength();
        if (tagToPartField != null) {
            // in case of reading partition field
            // add last field (partition field from tag name) to row
            String tag = extractTagName(split.getPath().getName(), tagToPartField);
            addTagToPartFieldRow = new JoinedRow();
            addTagToPartFieldRow.replace(null, GenericRow.of(BinaryString.fromString(tag)));
        } else {
            addTagToPartFieldRow = null;
        }

        // Calculate total row count for precise progress tracking
        long calculatedTotalRowCount = 0;
        boolean rowCountAvailable = true;
        try {
            DataSplit dataSplit = split.split();
            for (DataFileMeta file : dataSplit.dataFiles()) {
                long fileRowCount = file.rowCount();
                if (fileRowCount <= 0) {
                    // If any file has invalid row count, fall back to file size based estimation
                    rowCountAvailable = false;
                    break;
                }
                calculatedTotalRowCount += fileRowCount;
            }
        } catch (Exception e) {
            // If we can't access DataSplit or data files, fall back to size-based progress
            rowCountAvailable = false;
        }

        if (rowCountAvailable && calculatedTotalRowCount > 0) {
            this.totalRowCount = calculatedTotalRowCount;
            this.supportsRowCountTracking = true;
        } else {
            // Fall back to size-based progress (less accurate)
            this.totalRowCount = splitLength > 0 ? splitLength : 1; // Use split length as fallback
            this.supportsRowCountTracking = false;
        }
        this.completedRows = 0;
        this.progress = 0;
    }

    @Override
    public boolean next(Void key, RowDataContainer value) throws IOException {
        InternalRow rowData = iterator.next();

        if (rowData == null) {
            progress = 1;
            return false;
        } else {
            if (reusedProjectedRow != null) {
                value.set(reusedProjectedRow.replaceRow(rowData));
            } else {
                value.set(rowData);
            }
            if (addTagToPartFieldRow != null) {
                value.set(addTagToPartFieldRow.replace(value.get(), addTagToPartFieldRow.row2()));
            }

            // Update progress after successfully reading a record
            updateProgress();
            return true;
        }
    }

    private void updateProgress() {
        completedRows++;
        if (supportsRowCountTracking) {
            progress = Math.min(1.0f, (float) completedRows / totalRowCount);
        } else {
            // Size-based progress (less accurate, but better than nothing)
            progress = Math.min(1.0f, progress + (1.0f / Math.max(1, totalRowCount / 1024 / 1024)));
        }
    }

    @Override
    public Void createKey() {
        return null;
    }

    @Override
    public RowDataContainer createValue() {
        return new RowDataContainer();
    }

    @Override
    public long getPos() throws IOException {
        return (long) (splitLength * getProgress());
    }

    @Override
    public void close() throws IOException {
        try {
            iterator.close();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public float getProgress() throws IOException {
        // Precise progress tracking based on record counts
        // - If row counts are available from DataFileMeta.rowCount(), use row-based progress
        // - Otherwise, fall back to size-based progress estimation
        return progress;
    }

    public static RecordReader<Void, RowDataContainer> createRecordReader(
            PaimonInputSplit split, JobConf jobConf) throws IOException {
        FileStoreTable table = split.getTable();
        ReadBuilder readBuilder = table.newReadBuilder();
        createPredicate(table.schema(), jobConf, true).ifPresent(readBuilder::withFilter);
        List<String> paimonColumns = table.schema().fieldNames();
        return new PaimonRecordReader(
                readBuilder,
                split,
                paimonColumns,
                getHiveColumns(jobConf).orElse(paimonColumns),
                Arrays.asList(getSelectedColumns(jobConf)),
                table.coreOptions().tagToPartitionField());
    }

    private static Optional<List<String>> getHiveColumns(JobConf jobConf) {
        String columns = jobConf.get(IOConstants.SCHEMA_EVOLUTION_COLUMNS);
        if (columns == null) {
            columns = jobConf.get(hive_metastoreConstants.META_TABLE_COLUMNS);
        }
        String delimiter =
                jobConf.get(
                        // serdeConstants.COLUMN_NAME_DELIMITER is not defined in earlier Hive
                        // versions, so we use a constant string instead
                        "column.name.delimite", String.valueOf(SerDeUtils.COMMA));
        if (columns == null || delimiter == null) {
            return Optional.empty();
        } else {
            return Optional.of(Arrays.asList(columns.split(delimiter)));
        }
    }

    private static String[] getSelectedColumns(JobConf jobConf) {
        // when using tez engine or when same table is joined multiple times,
        // it is possible that some selected columns are duplicated
        return Arrays.stream(ColumnProjectionUtils.getReadColumnNames(jobConf))
                .distinct()
                .toArray(String[]::new);
    }
}
