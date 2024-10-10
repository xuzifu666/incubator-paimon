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
import org.apache.paimon.table.FileStoreTable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;

/** Reset consumer action for Flink. */
public class ResetConsumerAction extends TableActionBase {

    private final String consumerId;
    private Long nextSnapshotId;

    private static final Logger LOG = LoggerFactory.getLogger(ResetConsumerAction.class);

    protected ResetConsumerAction(
            String warehouse,
            String databaseName,
            String tableName,
            Map<String, String> catalogConfig,
            String consumerId) {
        super(warehouse, databaseName, tableName, catalogConfig);
        this.consumerId = consumerId;
    }

    public ResetConsumerAction withNextSnapshotIds(Long nextSnapshotId) {
        this.nextSnapshotId = nextSnapshotId;
        return this;
    }

    @Override
    public void run() throws Exception {
        FileStoreTable dataTable = (FileStoreTable) table;
        ConsumerManager consumerManager =
                new ConsumerManager(
                        dataTable.fileIO(),
                        dataTable.location(),
                        dataTable.snapshotManager().branch());
        if (Objects.isNull(nextSnapshotId)) {
            consumerManager.deleteConsumer(consumerId);
        } else {
            Long maxSnapshotId = dataTable.snapshotManager().latestSnapshotId();
            if (nextSnapshotId > maxSnapshotId) {
                LOG.warn(
                        String.format(
                                "Your nextSnapshotId is large than max snapshot id: %s, nextSnapshotId had change to %s",
                                maxSnapshotId, maxSnapshotId + 1));
                consumerManager.resetConsumer(consumerId, new Consumer(maxSnapshotId + 1));
            } else {
                consumerManager.resetConsumer(consumerId, new Consumer(nextSnapshotId));
            }
        }
    }
}
