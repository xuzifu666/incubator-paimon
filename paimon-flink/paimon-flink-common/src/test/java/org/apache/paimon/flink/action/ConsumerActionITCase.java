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

package org.apache.paimon.flink.action;

import org.apache.paimon.consumer.Consumer;
import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.table.planner.factories.TestValuesTableFactory.changelogRow;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.init;
import static org.apache.paimon.flink.util.ReadWriteTableTestUtil.testStreamingRead;
import static org.assertj.core.api.Assertions.assertThat;

/** IT cases for consumer management actions. */
public class ConsumerActionITCase extends ActionITCaseBase {

    @ParameterizedTest
    @ValueSource(strings = {"action", "procedure_indexed", "procedure_named"})
    public void testResetConsumer(String invoker) throws Exception {
        init(warehouse);

        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.BIGINT(), DataTypes.STRING()},
                        new String[] {"pk1", "col1"});
        FileStoreTable table =
                createFileStoreTable(
                        rowType,
                        Collections.emptyList(),
                        Collections.singletonList("pk1"),
                        Collections.emptyList(),
                        Collections.emptyMap());

        StreamWriteBuilder writeBuilder = table.newStreamWriteBuilder().withCommitUser(commitUser);
        write = writeBuilder.newWrite();
        commit = writeBuilder.newCommit();

        // 3 snapshots
        writeData(rowData(1L, BinaryString.fromString("Hi")));
        writeData(rowData(2L, BinaryString.fromString("Hello")));
        writeData(rowData(3L, BinaryString.fromString("Paimon")));

        // use consumer streaming read table
        testStreamingRead(
                        "SELECT * FROM `"
                                + tableName
                                + "` /*+ OPTIONS('consumer-id'='myid','consumer.expiration-time'='3h') */",
                        Arrays.asList(
                                changelogRow("+I", 1L, "Hi"),
                                changelogRow("+I", 2L, "Hello"),
                                changelogRow("+I", 3L, "Paimon")))
                .close();

        Thread.sleep(1000);
        ConsumerManager consumerManager = new ConsumerManager(table.fileIO(), table.location());
        Optional<Consumer> consumer1 = consumerManager.consumer("myid");
        assertThat(consumer1).isPresent();
        assertThat(consumer1.get().nextSnapshot()).isEqualTo(4);

        List<String> args =
                Arrays.asList(
                        "reset_consumer",
                        "--warehouse",
                        warehouse,
                        "--database",
                        database,
                        "--table",
                        tableName,
                        "--consumer_id",
                        "myid",
                        "--next_snapshot",
                        "1");
        // reset consumer
        switch (invoker) {
            case "action":
                createAction(ResetConsumerAction.class, args).run();
                break;
            case "procedure_indexed":
                executeSQL(
                        String.format(
                                "CALL sys.reset_consumer('%s.%s', 'myid', 1)",
                                database, tableName));
                break;
            case "procedure_named":
                executeSQL(
                        String.format(
                                "CALL sys.reset_consumer(`table` => '%s.%s', consumer_id => 'myid', next_snapshot_id => cast(1 as bigint))",
                                database, tableName));
                break;
            default:
                throw new UnsupportedOperationException(invoker);
        }
        Optional<Consumer> consumer2 = consumerManager.consumer("myid");
        assertThat(consumer2).isPresent();
        assertThat(consumer2.get().nextSnapshot()).isEqualTo(1);

        // delete consumer
        switch (invoker) {
            case "action":
                createAction(ResetConsumerAction.class, args.subList(0, 9)).run();
                break;
            case "procedure_indexed":
                executeSQL(
                        String.format(
                                "CALL sys.reset_consumer('%s.%s', 'myid')", database, tableName));
                break;
            case "procedure_named":
                executeSQL(
                        String.format(
                                "CALL sys.reset_consumer(`table` => '%s.%s', consumer_id => 'myid')",
                                database, tableName));
                break;
            default:
                throw new UnsupportedOperationException(invoker);
        }
        Optional<Consumer> consumer3 = consumerManager.consumer("myid");
        assertThat(consumer3).isNotPresent();

        // test for snapshot 0
        args =
                Arrays.asList(
                        "reset_consumer",
                        "--warehouse",
                        warehouse,
                        "--database",
                        database,
                        "--table",
                        tableName,
                        "--consumer_id",
                        "myid",
                        "--next_snapshot",
                        "2");
        // reset consumer
        switch (invoker) {
            case "action":
                createAction(ResetConsumerAction.class, args).run();
                break;
            case "procedure_indexed":
                executeSQL(
                        String.format(
                                "CALL sys.reset_consumer('%s.%s', 'myid', 2)",
                                database, tableName));
                break;
            case "procedure_named":
                executeSQL(
                        String.format(
                                "CALL sys.reset_consumer(`table` => '%s.%s', consumer_id => 'myid', next_snapshot_id => cast(2 as bigint))",
                                database, tableName));
                break;
            default:
                throw new UnsupportedOperationException(invoker);
        }

        // 3 snapshots
        writeData(rowData(4L, BinaryString.fromString("aa")));
        writeData(rowData(5L, BinaryString.fromString("bb")));
        writeData(rowData(6L, BinaryString.fromString("cc")));

        Optional<Consumer> consumer4 = consumerManager.consumer("myid");
        assertThat(consumer4).isPresent();
        assertThat(consumer4.get().nextSnapshot()).isEqualTo(2);

        // use consumer streaming read table
        testStreamingRead(
                        "SELECT * FROM `"
                                + tableName
                                + "` /*+ OPTIONS('consumer-id'='myid','consumer.expiration-time'='3h') */",
                        Arrays.asList(
                                changelogRow("+I", 2L, "Hello"),
                                changelogRow("+I", 3L, "Paimon"),
                                changelogRow("+I", 4L, "aa"),
                                changelogRow("+I", 5L, "bb"),
                                changelogRow("+I", 6L, "cc")))
                .close();

        // 3 snapshots
        writeData(rowData(7L, BinaryString.fromString("dd")));
        writeData(rowData(8L, BinaryString.fromString("ee")));
        writeData(rowData(9L, BinaryString.fromString("ff")));

        // test for snapshot 10
        args =
                Arrays.asList(
                        "reset_consumer",
                        "--warehouse",
                        warehouse,
                        "--database",
                        database,
                        "--table",
                        tableName,
                        "--consumer_id",
                        "myid",
                        "--next_snapshot",
                        "15");
        // reset consumer
        switch (invoker) {
            case "action":
                createAction(ResetConsumerAction.class, args).run();
                break;
            case "procedure_indexed":
                executeSQL(
                        String.format(
                                "CALL sys.reset_consumer('%s.%s', 'myid', 15)",
                                database, tableName));
                break;
            case "procedure_named":
                executeSQL(
                        String.format(
                                "CALL sys.reset_consumer(`table` => '%s.%s', consumer_id => 'myid', next_snapshot_id => cast(15 as bigint))",
                                database, tableName));
                break;
            default:
                throw new UnsupportedOperationException(invoker);
        }

        Optional<Consumer> consumer5 = consumerManager.consumer("myid");
        assertThat(consumer5).isPresent();
        assertThat(consumer5.get().nextSnapshot()).isEqualTo(12);

        // after 10 s add a snapshot
        new Thread(
                        new Runnable() {
                            @Override
                            public void run() {
                                try {
                                    Thread.sleep(10000);
                                    writeData(rowData(10L, BinaryString.fromString("gg")));
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            }
                        })
                .start();

        // use consumer streaming read table
        testStreamingRead(
                        "SELECT * FROM `"
                                + tableName
                                + "` /*+ OPTIONS('consumer-id'='myid','consumer.expiration-time'='3h') */",
                        Arrays.asList(changelogRow("+I", 10L, "gg")))
                .close();
    }

    @ParameterizedTest
    @ValueSource(strings = {"action", "procedure_indexed", "procedure_named"})
    public void testResetBranchConsumer(String invoker) throws Exception {
        init(warehouse);

        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.BIGINT(), DataTypes.STRING()},
                        new String[] {"pk1", "col1"});
        FileStoreTable table =
                createFileStoreTable(
                        rowType,
                        Collections.emptyList(),
                        Collections.singletonList("pk1"),
                        Collections.emptyList(),
                        Collections.emptyMap());

        StreamWriteBuilder writeBuilder = table.newStreamWriteBuilder().withCommitUser(commitUser);
        write = writeBuilder.newWrite();
        commit = writeBuilder.newCommit();

        // 3 snapshots
        writeData(rowData(1L, BinaryString.fromString("Hi")));
        writeData(rowData(2L, BinaryString.fromString("Hello")));
        writeData(rowData(3L, BinaryString.fromString("Paimon")));

        table.createTag("tag", 3);
        String branchName = "b1";
        table.createBranch("b1", "tag");
        String branchTableName = tableName + "$branch_b1";

        // use consumer streaming read table
        testStreamingRead(
                        "SELECT * FROM `"
                                + branchTableName
                                + "` /*+ OPTIONS('consumer-id'='myid','consumer.expiration-time'='3h') */",
                        Arrays.asList(
                                changelogRow("+I", 1L, "Hi"),
                                changelogRow("+I", 2L, "Hello"),
                                changelogRow("+I", 3L, "Paimon")))
                .close();

        ConsumerManager consumerManager =
                new ConsumerManager(table.fileIO(), table.location(), branchName);
        Optional<Consumer> consumer1 = consumerManager.consumer("myid");
        assertThat(consumer1).isPresent();
        assertThat(consumer1.get().nextSnapshot()).isEqualTo(4);

        List<String> args =
                Arrays.asList(
                        "reset_consumer",
                        "--warehouse",
                        warehouse,
                        "--database",
                        database,
                        "--table",
                        branchTableName,
                        "--consumer_id",
                        "myid",
                        "--next_snapshot",
                        "1");
        // reset consumer
        switch (invoker) {
            case "action":
                createAction(ResetConsumerAction.class, args).run();
                break;
            case "procedure_indexed":
                executeSQL(
                        String.format(
                                "CALL sys.reset_consumer('%s.%s', 'myid', 1)",
                                database, branchTableName));
                break;
            case "procedure_named":
                executeSQL(
                        String.format(
                                "CALL sys.reset_consumer(`table` => '%s.%s', consumer_id => 'myid', next_snapshot_id => cast(1 as bigint))",
                                database, branchTableName));
                break;
            default:
                throw new UnsupportedOperationException(invoker);
        }
        Optional<Consumer> consumer2 = consumerManager.consumer("myid");
        assertThat(consumer2).isPresent();
        assertThat(consumer2.get().nextSnapshot()).isEqualTo(1);

        // delete consumer
        switch (invoker) {
            case "action":
                createAction(ResetConsumerAction.class, args.subList(0, 9)).run();
                break;
            case "procedure_indexed":
                executeSQL(
                        String.format(
                                "CALL sys.reset_consumer('%s.%s', 'myid')",
                                database, branchTableName));
                break;
            case "procedure_named":
                executeSQL(
                        String.format(
                                "CALL sys.reset_consumer(`table` => '%s.%s', consumer_id => 'myid')",
                                database, branchTableName));
                break;
            default:
                throw new UnsupportedOperationException(invoker);
        }
        Optional<Consumer> consumer3 = consumerManager.consumer("myid");
        assertThat(consumer3).isNotPresent();
    }
}
