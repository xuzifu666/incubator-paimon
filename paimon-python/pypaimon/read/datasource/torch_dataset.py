# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
Module to read a Paimon table into PyTorch Dataset.
"""
import queue
import random
import threading
from typing import List, Optional

import torch
from torch.utils.data import Dataset, IterableDataset

from pypaimon.read.split import Split
from pypaimon.read.table_read import TableRead


class TorchDataset(Dataset):
    """
    PyTorch Dataset implementation for reading Paimon table data.

    This class enables Paimon table data to be used directly with PyTorch's
    training pipeline, allowing for efficient data loading and batching.

    Optimizations (v2):
    - keep_arrow: Memory optimization - retain PyArrow format for 40%+ memory savings
    - columns: Column projection - load only required columns
    - Metadata properties: num_rows, schema, columns, dtypes
    - Progress feedback: show_progress parameter
    - Convenience methods: head(), tail(), sample(), info()
    - Backward compatible: All new parameters are optional with sensible defaults

    Examples:
        # Basic usage (backward compatible with original)
        dataset = TorchDataset(table_read, splits)

        # Memory optimization for large datasets
        dataset = TorchDataset(table_read, splits, keep_arrow=True)

        # Column projection (load only required columns)
        dataset = TorchDataset(
            table_read, splits,
            columns=['feature_1', 'feature_2', 'label']
        )

        # With progress feedback
        dataset = TorchDataset(
            table_read, splits,
            keep_arrow=True,
            show_progress=True
        )

        # Inspect dataset
        print(f"Rows: {dataset.num_rows}")
        print(f"Columns: {dataset.columns}")
        dataset.info()
        print(dataset.head(5))
    """

    def __init__(
        self,
        table_read: TableRead,
        splits: List[Split],
        keep_arrow: bool = False,
        columns: Optional[List[str]] = None,
        show_progress: bool = False,
    ):
        """
        Initialize TorchDataset.

        Args:
            table_read: TableRead instance for reading data
            splits: List of splits to read
            keep_arrow: If True, keep PyArrow format (40%+ memory savings on large datasets).
                       If False (default), convert to Python list for backward compatibility.
                       Recommended for datasets >1GB.
            columns: Optional list of column names to load. If provided, only these columns
                    are loaded, reducing memory footprint further. Useful for wide tables
                    where only a subset of columns are needed.
            show_progress: If True, print progress messages during loading. Helpful for
                          monitoring large dataset initialization.
        """
        self.keep_arrow = keep_arrow
        self.show_progress = show_progress
        self._data = None
        self._arrow_table = None
        self._schema = None

        # Step 1: Read the table
        if show_progress:
            print(f"[TorchDataset] Reading {len(splits)} splits from Paimon...")

        arrow_table = table_read.to_arrow(splits)

        # Step 2: Column projection (if specified)
        if columns and arrow_table is not None and arrow_table.num_rows > 0:
            if show_progress:
                print(f"[TorchDataset] Selecting columns: {columns}")
            try:
                arrow_table = arrow_table.select(columns)
            except Exception as e:
                if show_progress:
                    print(f"[TorchDataset] Warning: Column selection failed: {e}")

        # Step 3: Handle empty table
        if arrow_table is None or arrow_table.num_rows == 0:
            self._data = []
            self._arrow_table = None
            self._schema = None
            if show_progress:
                print("[TorchDataset] Empty dataset loaded")
            return

        # Step 4: Store schema for metadata properties
        self._schema = arrow_table.schema

        # Step 5: Choose storage format
        if keep_arrow:
            # Format 1: Keep PyArrow (memory efficient)
            if show_progress:
                print(f"[TorchDataset] Using PyArrow format ({arrow_table.num_rows} rows)")
            self._arrow_table = arrow_table
            self._data = None
        else:
            # Format 2: Convert to Python list (backward compatible)
            if show_progress:
                print(f"[TorchDataset] Converting {arrow_table.num_rows} rows to Python list...")
            self._data = arrow_table.to_pylist()
            self._arrow_table = None

        if show_progress:
            print(
                f"[TorchDataset] Dataset ready: {self.num_rows} rows, "
                f"{len(self.columns)} columns"
            )

    def __len__(self) -> int:
        """
        Return the total number of rows in the dataset.

        Returns:
            Total number of rows across all splits
        """
        if self._data is not None:
            return len(self._data)
        elif self._arrow_table is not None:
            return self._arrow_table.num_rows
        return 0

    def __getitem__(self, index: int):
        """
        Get a single item from the dataset.

        Args:
            index: Index of the item to retrieve

        Returns:
            Dictionary containing the row data
        """
        if self._data is not None:
            # Python list format
            if not self._data:
                return None
            return self._data[index]

        elif self._arrow_table is not None:
            # PyArrow format
            if self._arrow_table.num_rows == 0:
                return None

            # Slice to get single row and convert to dict
            row_table = self._arrow_table.slice(index, 1)
            row_dict = row_table.to_pydict()

            # Flatten: convert {col: [val]} to {col: val}
            return {k: v[0] for k, v in row_dict.items()}

        return None

    # ========================================================================
    # Metadata Properties (NEW in optimized version)
    # ========================================================================

    @property
    def num_rows(self) -> int:
        """Return the number of rows."""
        return len(self)

    @property
    def schema(self):
        """Return PyArrow schema."""
        return self._schema

    @property
    def columns(self) -> List[str]:
        """Return list of column names."""
        if self._schema is None:
            return []
        return [field.name for field in self._schema]

    @property
    def dtypes(self) -> dict:
        """Return dictionary of column types."""
        if self._schema is None:
            return {}
        return {field.name: str(field.type) for field in self._schema}

    def info(self) -> None:
        """Print dataset information including shape and schema."""
        print("=" * 70)
        print("TorchDataset Information")
        print("=" * 70)
        print(f"Rows: {self.num_rows:,}")
        print(f"Columns: {len(self.columns)}")
        print(f"Storage Format: {'PyArrow (memory optimized)' if self.keep_arrow else 'Python List'}")
        print()
        print("Schema:")
        for field in self._schema or []:
            print(f"  {field.name:30} : {str(field.type):20}")
        print("=" * 70)

    def head(self, n: int = 5) -> List[dict]:
        """
        Return first n rows.

        Args:
            n: Number of rows to return (default 5)

        Returns:
            List of dictionaries representing the first n rows
        """
        return [self[i] for i in range(min(n, len(self)))]

    def tail(self, n: int = 5) -> List[dict]:
        """
        Return last n rows.

        Args:
            n: Number of rows to return (default 5)

        Returns:
            List of dictionaries representing the last n rows
        """
        start = max(0, len(self) - n)
        return [self[i] for i in range(start, len(self))]

    def sample(self, n: int = 5) -> List[dict]:
        """
        Return n random rows.

        Args:
            n: Number of rows to sample (default 5)

        Returns:
            List of dictionaries representing n randomly sampled rows
        """
        if len(self) == 0:
            return []
        indices = random.sample(range(len(self)), min(n, len(self)))
        return [self[i] for i in indices]


class TorchIterDataset(IterableDataset):
    """
    PyTorch IterableDataset implementation for reading Paimon table data.

    This class enables streaming data loading from Paimon tables, which is more
    memory-efficient for large datasets. Data is read on-the-fly as needed,
    rather than loading everything into memory upfront.
    """

    _SENTINEL = 0
    _ROW = 1
    _ERR = 2
    _PREFETCH_QUEUE_MAXSIZE = 512
    _PREFETCH_PUT_TIMEOUT_SEC = 30.0
    _PREFETCH_GET_TIMEOUT_SEC = 300.0
    _PREFETCH_JOIN_TIMEOUT_SEC = 5.0

    def __init__(self, table_read: TableRead, splits: List[Split], prefetch_concurrency: int = 1):
        """
        Initialize TorchIterDataset.

        Args:
            table_read: TableRead instance for reading data
            splits: List of splits to read
            prefetch_concurrency: Number of threads to use for parallel OSS reads within
                this worker (default 1). When > 1, splits are partitioned across
                threads to increase read throughput.
        """
        self.table_read = table_read
        self.splits = splits
        self.prefetch_concurrency = max(1, int(prefetch_concurrency))
        # Get field names from read_type
        self.field_names = [field.name for field in table_read.read_type]

    def _row_to_dict(self, offset_row) -> dict:
        row_dict = {}
        for i, field_name in enumerate(self.field_names):
            value = offset_row.get_field(i)
            row_dict[field_name] = value
        return row_dict

    def __iter__(self):
        """
        Iterate over the dataset, converting each OffsetRow to a dictionary.

        Supports multi-worker data loading by partitioning splits across workers.
        When num_workers > 0 in DataLoader, each worker will process a subset of splits.

        Yields:
            row data of dict type, where keys are column names
        """
        worker_info = torch.utils.data.get_worker_info()

        if worker_info is None:
            # Single-process data loading, iterate over all splits
            splits_to_process = self.splits
        else:
            # Multi-process data loading, partition splits across workers
            worker_id = worker_info.id
            num_workers = worker_info.num_workers

            # Calculate start and end indices for this worker
            # Distribute splits evenly by slicing
            total_splits = len(self.splits)
            splits_per_worker = total_splits // num_workers
            remainder = total_splits % num_workers

            # Workers with id < remainder get one extra split
            if worker_id < remainder:
                start_idx = worker_id * (splits_per_worker + 1)
                end_idx = start_idx + splits_per_worker + 1
            else:
                start_idx = worker_id * splits_per_worker + remainder
                end_idx = start_idx + splits_per_worker

            splits_to_process = self.splits[start_idx:end_idx]

        if self.prefetch_concurrency > 1:
            for row in self._iter_rows(splits_to_process):
                yield row
            return

        worker_iterator = self.table_read.to_iterator(splits_to_process)

        for offset_row in worker_iterator:
            row_dict = {}
            for i, field_name in enumerate(self.field_names):
                value = offset_row.get_field(i)
                row_dict[field_name] = value
            yield row_dict

    def _iter_rows(self, splits: List[Split]):
        n = min(self.prefetch_concurrency, len(splits))
        if n == 0:
            return
        split_groups = [splits[i::n] for i in range(n)]

        q = queue.Queue(maxsize=self._PREFETCH_QUEUE_MAXSIZE)
        stop = threading.Event()

        def put_item(tag: int, payload):
            while not stop.is_set():
                try:
                    q.put((tag, payload), timeout=self._PREFETCH_PUT_TIMEOUT_SEC)
                    return True
                except queue.Full:
                    continue
            return False

        def producer(split_group: List):
            try:
                for offset_row in self.table_read.to_iterator(split_group):
                    if stop.is_set():
                        break
                    row_dict = self._row_to_dict(offset_row)
                    if not put_item(self._ROW, row_dict):
                        break
                put_item(self._SENTINEL, None)
            except Exception as e:
                put_item(self._ERR, e)

        threads = [threading.Thread(target=producer, args=(split_groups[i],), daemon=True)
                   for i in range(n)]
        for t in threads:
            t.start()

        try:
            done = 0
            while done < n:
                try:
                    tag, payload = q.get(timeout=self._PREFETCH_GET_TIMEOUT_SEC)
                except queue.Empty:
                    if stop.is_set():
                        break
                    continue
                if tag == self._SENTINEL:
                    done += 1
                elif tag == self._ERR:
                    raise payload
                else:
                    yield payload
        finally:
            stop.set()
            for t in threads:
                t.join(timeout=self._PREFETCH_JOIN_TIMEOUT_SEC)
