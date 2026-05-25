#!/usr/bin/env python3
"""
Test cases for optimized TorchDataset

Tests the following improvements:
- keep_arrow parameter (memory optimization)
- columns parameter (column projection)
- Metadata properties (num_rows, schema, columns, dtypes)
- Convenience methods (head, tail, sample, info)
- Backward compatibility
"""

import unittest
import tempfile
import shutil
import os

import pyarrow as pa
from pypaimon import CatalogFactory, Schema
from pypaimon.read.datasource.torch_dataset import TorchDataset


class TestTorchDatasetOptimized(unittest.TestCase):
    """Test cases for optimized TorchDataset"""

    @classmethod
    def setUpClass(cls):
        """Set up test fixtures"""
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')

        # Create catalog
        cls.catalog = CatalogFactory.create({'warehouse': cls.warehouse})
        cls.catalog.create_database('default', True)

        # Create test schema
        cls.pa_schema = pa.schema([
            ('id', pa.int32()),
            ('name', pa.string()),
            ('age', pa.int32()),
            ('score', pa.float32()),
            ('city', pa.string()),
        ])

        # Create test table
        cls.catalog.create_table('default.test_table', Schema.from_pyarrow_schema(
            cls.pa_schema,
            partition_keys=['city']
        ), False)

        # Write test data
        cls.table = cls.catalog.get_table('default.test_table')
        write_builder = cls.table.new_batch_write_builder()

        test_data = {
            'id': [1, 2, 3, 4, 5],
            'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
            'age': [25, 30, 35, 28, 32],
            'score': [85.5, 90.0, 78.5, 92.5, 88.0],
            'city': ['NY', 'LA', 'NY', 'LA', 'NY'],
        }

        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        pa_table = pa.Table.from_pydict(test_data, schema=cls.pa_schema)
        table_write.write_arrow(pa_table)
        table_commit.commit(table_write.prepare_commit())

    @classmethod
    def tearDownClass(cls):
        """Clean up test fixtures"""
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def setUp(self):
        """Set up for each test"""
        read_builder = self.table.new_read_builder()
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        self.splits = table_scan.plan().splits()
        self.table_read = table_read

    def test_backward_compatibility(self):
        """Test that original usage still works"""
        # Original usage (should work exactly as before)
        dataset = TorchDataset(self.table_read, self.splits)

        self.assertEqual(len(dataset), 5)
        self.assertIsNotNone(dataset[0])
        self.assertEqual(dataset[0]['name'], 'Alice')

    def test_keep_arrow_parameter(self):
        """Test keep_arrow parameter for memory optimization"""
        dataset = TorchDataset(self.table_read, self.splits, keep_arrow=True)

        # Check that PyArrow format is used
        self.assertIsNotNone(dataset._arrow_table)
        self.assertIsNone(dataset._data)

        # Check that data is still accessible
        self.assertEqual(len(dataset), 5)
        self.assertEqual(dataset[0]['name'], 'Alice')
        self.assertEqual(dataset[4]['name'], 'Eve')

    def test_columns_parameter(self):
        """Test column projection"""
        dataset = TorchDataset(
            self.table_read,
            self.splits,
            columns=['id', 'name', 'age']
        )

        # Check that only specified columns are loaded
        self.assertEqual(set(dataset.columns), {'id', 'name', 'age'})

        # Check data
        row = dataset[0]
        self.assertIn('id', row)
        self.assertIn('name', row)
        self.assertIn('age', row)
        self.assertNotIn('score', row)
        self.assertNotIn('city', row)

    def test_columns_with_keep_arrow(self):
        """Test columns parameter combined with keep_arrow"""
        dataset = TorchDataset(
            self.table_read,
            self.splits,
            keep_arrow=True,
            columns=['name', 'score']
        )

        # Check both optimizations are active
        self.assertIsNotNone(dataset._arrow_table)
        self.assertIsNone(dataset._data)
        self.assertEqual(set(dataset.columns), {'name', 'score'})

        row = dataset[0]
        self.assertEqual(row['name'], 'Alice')
        self.assertAlmostEqual(row['score'], 85.5, places=1)

    def test_metadata_properties(self):
        """Test metadata properties"""
        dataset = TorchDataset(self.table_read, self.splits)

        # Test num_rows
        self.assertEqual(dataset.num_rows, 5)

        # Test columns
        self.assertEqual(set(dataset.columns), {'id', 'name', 'age', 'score', 'city'})

        # Test schema
        self.assertIsNotNone(dataset.schema)

        # Test dtypes
        dtypes = dataset.dtypes
        self.assertIn('id', dtypes)
        self.assertIn('name', dtypes)
        self.assertIn('score', dtypes)

    def test_head_method(self):
        """Test head() convenience method"""
        dataset = TorchDataset(self.table_read, self.splits)

        # Get first 3 rows
        head_data = dataset.head(3)
        self.assertEqual(len(head_data), 3)
        self.assertEqual(head_data[0]['name'], 'Alice')
        self.assertEqual(head_data[1]['name'], 'Bob')
        self.assertEqual(head_data[2]['name'], 'Charlie')

    def test_tail_method(self):
        """Test tail() convenience method"""
        dataset = TorchDataset(self.table_read, self.splits)

        # Get last 2 rows
        tail_data = dataset.tail(2)
        self.assertEqual(len(tail_data), 2)
        self.assertEqual(tail_data[0]['name'], 'David')
        self.assertEqual(tail_data[1]['name'], 'Eve')

    def test_sample_method(self):
        """Test sample() convenience method"""
        dataset = TorchDataset(self.table_read, self.splits)

        # Sample 3 rows
        sampled = dataset.sample(3)
        self.assertEqual(len(sampled), 3)

        # All sampled rows should be valid
        for row in sampled:
            self.assertIn(row['name'], ['Alice', 'Bob', 'Charlie', 'David', 'Eve'])

    def test_info_method(self):
        """Test info() method (just ensure it doesn't crash)"""
        dataset = TorchDataset(self.table_read, self.splits)

        # info() should not raise any exception
        try:
            dataset.info()
        except Exception as e:
            self.fail(f"info() raised {type(e).__name__}: {e}")

    def test_empty_dataset(self):
        """Test handling of empty dataset"""
        # Create empty table
        empty_schema = Schema.from_pyarrow_schema(
            pa.schema([('id', pa.int32()), ('name', pa.string())]),
        )
        self.catalog.create_table('default.empty_table', empty_schema, False)
        empty_table = self.catalog.get_table('default.empty_table')

        read_builder = empty_table.new_read_builder()
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        splits = table_scan.plan().splits()

        dataset = TorchDataset(table_read, splits)

        # Should handle empty dataset gracefully
        self.assertEqual(len(dataset), 0)
        self.assertEqual(dataset.columns, [])
        self.assertEqual(dataset.head(), [])

    def test_large_row_count(self):
        """Test with larger dataset"""
        # Create larger test data
        large_schema = Schema.from_pyarrow_schema(
            pa.schema([
                ('id', pa.int32()),
                ('value', pa.float32()),
            ])
        )
        self.catalog.create_table('default.large_table', large_schema, False)
        large_table = self.catalog.get_table('default.large_table')

        # Write 1000 rows
        write_builder = large_table.new_batch_write_builder()
        for batch_idx in range(10):
            table_write = write_builder.new_write()
            table_commit = write_builder.new_commit()

            data = {
                'id': list(range(batch_idx * 100, (batch_idx + 1) * 100)),
                'value': [float(i) * 1.5 for i in range(batch_idx * 100, (batch_idx + 1) * 100)],
            }
            pa_table = pa.Table.from_pydict(data, schema=large_schema)
            table_write.write_arrow(pa_table)
            table_commit.commit(table_write.prepare_commit())

        # Read back
        read_builder = large_table.new_read_builder()
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        splits = table_scan.plan().splits()

        # Test with keep_arrow
        dataset = TorchDataset(table_read, splits, keep_arrow=True, show_progress=False)

        self.assertEqual(len(dataset), 1000)
        self.assertEqual(dataset[0]['id'], 0)
        self.assertEqual(dataset[999]['id'], 999)

    def test_mixed_optimizations(self):
        """Test combining multiple optimizations"""
        dataset = TorchDataset(
            self.table_read,
            self.splits,
            keep_arrow=True,
            columns=['id', 'name'],
            show_progress=False
        )

        # Verify all optimizations are active
        self.assertIsNotNone(dataset._arrow_table)
        self.assertIsNone(dataset._data)
        self.assertEqual(set(dataset.columns), {'id', 'name'})
        self.assertEqual(dataset.num_rows, 5)

        # Verify data access
        row = dataset[0]
        self.assertEqual(row['id'], 1)
        self.assertEqual(row['name'], 'Alice')
        self.assertNotIn('age', row)


class TestTorchDatasetPerformance(unittest.TestCase):
    """Performance comparison tests"""

    @classmethod
    def setUpClass(cls):
        """Set up performance test fixtures"""
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')

        cls.catalog = CatalogFactory.create({'warehouse': cls.warehouse})
        cls.catalog.create_database('default', True)

        # Create wide table (100 columns)
        columns = [(f'col_{i}', pa.float32()) for i in range(100)]
        cls.wide_schema = pa.schema(columns)

        cls.catalog.create_table('default.wide_table', Schema.from_pyarrow_schema(
            cls.wide_schema
        ), False)

        cls.wide_table = cls.catalog.get_table('default.wide_table')

        # Write data
        write_builder = cls.wide_table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()

        data = {f'col_{i}': [float(j * i) for j in range(100)] for i in range(100)}
        pa_table = pa.Table.from_pydict(data, schema=cls.wide_schema)
        table_write.write_arrow(pa_table)
        table_commit.commit(table_write.prepare_commit())

    @classmethod
    def tearDownClass(cls):
        """Clean up"""
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def test_wide_table_with_column_projection(self):
        """Test memory savings with column projection on wide table"""
        read_builder = self.wide_table.new_read_builder()
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        splits = table_scan.plan().splits()

        # Load only 10 columns out of 100
        dataset = TorchDataset(
            table_read,
            splits,
            columns=[f'col_{i}' for i in range(10)],
            keep_arrow=True
        )

        self.assertEqual(len(dataset.columns), 10)
        self.assertEqual(dataset.num_rows, 100)

        # Verify only requested columns are present
        row = dataset[0]
        self.assertEqual(len(row), 10)


if __name__ == '__main__':
    unittest.main()
