package com.sequenceiq.hbase.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class HBaseOperationsTest {

    private static final String TABLE_NAME = "table";
    private static final String ROW_KEY = "row1";
    private static final String FAMILY = "cf";
    private static final String COLUMN = "column";

    @InjectMocks
    private HBaseOperations hBaseOperations;

    @Mock
    private HBaseTablePool tablePool;
    @Mock
    private HTableInterface tableInterface;
    @Captor
    private ArgumentCaptor<List<Put>> putCaptor;
    @Captor
    private ArgumentCaptor<Get> getCaptor;
    @Captor
    private ArgumentCaptor<Scan> scanCaptor;

    @Test
    public void testPutForNull() throws IOException {
        // GIVEN
        when(tablePool.getHTable(TABLE_NAME)).thenReturn(tableInterface);
        HBaseRow row = null;

        // WHEN
        hBaseOperations.put(TABLE_NAME, row, true);

        // THEN
        verify(tableInterface).put(putCaptor.capture());
        List<Put> puts = putCaptor.getValue();
        assertEquals(0, puts.size());
    }

    @Test
    public void testPutForCollectionContainsNull() throws IOException {
        // GIVEN
        when(tablePool.getHTable(TABLE_NAME)).thenReturn(tableInterface);
        HBaseRow row1 = null;
        HBaseRow row2 = new HBaseRow(ROW_KEY, FAMILY, COLUMN, "val");

        // WHEN
        hBaseOperations.put(TABLE_NAME, Arrays.asList(row1, row2), true);

        // THEN
        verify(tableInterface).put(putCaptor.capture());
        List<Put> puts = putCaptor.getValue();
        assertEquals(1, puts.size());
    }

    @Test
    public void testPutForSingleColumnFamilySingleColumn() throws IOException {
        // GIVEN
        when(tablePool.getHTable(TABLE_NAME)).thenReturn(tableInterface);

        // WHEN
        hBaseOperations.put(TABLE_NAME, ROW_KEY, FAMILY, COLUMN, "value", true);

        // THEN
        verify(tableInterface).put(putCaptor.capture());
        List<Put> puts = putCaptor.getValue();
        Put put = puts.get(0);
        List<Cell> cells = put.get(FAMILY.getBytes(), COLUMN.getBytes());
        Cell cell = cells.get(0);
        assertEquals(1, puts.size());
        assertEquals(1, cells.size());
        assertEquals(ROW_KEY, Bytes.toString(put.getRow()));
        assertEquals("value", Bytes.toString(CellUtil.cloneValue(cell)));
    }

    @Test
    public void testPutForSingleColumnFamilyMultipleColumns() throws IOException {
        // GIVEN
        when(tablePool.getHTable(TABLE_NAME)).thenReturn(tableInterface);
        HBaseRow row = new HBaseRow(ROW_KEY, FAMILY, COLUMN, "val");
        row.addValue(FAMILY, "column2", "val2");

        // WHEN
        hBaseOperations.put(TABLE_NAME, row, true);

        // THEN
        verify(tableInterface).put(putCaptor.capture());
        List<Put> puts = putCaptor.getValue();
        Put put = puts.get(0);
        List<Cell> cells1 = put.get(FAMILY.getBytes(), COLUMN.getBytes());
        List<Cell> cells2 = put.get(FAMILY.getBytes(), "column2".getBytes());
        Cell cell1 = cells1.get(0);
        Cell cell2 = cells2.get(0);
        assertEquals(1, puts.size());
        assertEquals(1, cells2.size());
        assertEquals(ROW_KEY, Bytes.toString(put.getRow()));
        assertEquals("val", Bytes.toString(CellUtil.cloneValue(cell1)));
        assertEquals("val2", Bytes.toString(CellUtil.cloneValue(cell2)));
    }

    @Test
    public void testPutForMultipleColumnFamiliesMultipleColumns() throws IOException {
        // GIVEN
        when(tablePool.getHTable(TABLE_NAME)).thenReturn(tableInterface);
        HBaseRow row = new HBaseRow(ROW_KEY, FAMILY, COLUMN, "val");
        row.addValue(FAMILY, "column2", "val2");
        row.addValue("cf2", COLUMN, "val3");

        // WHEN
        hBaseOperations.put(TABLE_NAME, row, true);

        // THEN
        verify(tableInterface).put(putCaptor.capture());
        List<Put> puts = putCaptor.getValue();
        Put put = puts.get(0);
        List<Cell> cells1 = put.get(FAMILY.getBytes(), COLUMN.getBytes());
        List<Cell> cells2 = put.get(FAMILY.getBytes(), "column2".getBytes());
        List<Cell> cells3 = put.get("cf2".getBytes(), COLUMN.getBytes());
        Cell cell1 = cells1.get(0);
        Cell cell2 = cells2.get(0);
        Cell cell3 = cells3.get(0);
        assertEquals(1, puts.size());
        assertEquals(1, cells1.size());
        assertEquals(1, cells2.size());
        assertEquals(1, cells3.size());
        assertEquals(ROW_KEY, Bytes.toString(put.getRow()));
        assertEquals("val", Bytes.toString(CellUtil.cloneValue(cell1)));
        assertEquals("val2", Bytes.toString(CellUtil.cloneValue(cell2)));
        assertEquals("val3", Bytes.toString(CellUtil.cloneValue(cell3)));
    }

    @Test
    public void testPutForMultipleRowMultipleColumnFamiliesMultipleColumns() throws IOException {
        // GIVEN
        when(tablePool.getHTable(TABLE_NAME)).thenReturn(tableInterface);
        HBaseRow row1 = new HBaseRow(ROW_KEY, FAMILY, COLUMN, "val");
        row1.addValue(FAMILY, "column2", "val2");
        row1.addValue("cf2", COLUMN, "val3");
        HBaseRow row2 = new HBaseRow(ROW_KEY, FAMILY, COLUMN, "test");

        // WHEN
        hBaseOperations.put(TABLE_NAME, Arrays.asList(row1, row2), true);

        // THEN
        verify(tableInterface).put(putCaptor.capture());
        List<Put> puts = putCaptor.getValue();
        Put put1 = puts.get(0);
        Put put2 = puts.get(1);
        List<Cell> cells1 = put1.get(FAMILY.getBytes(), COLUMN.getBytes());
        List<Cell> cells2 = put1.get(FAMILY.getBytes(), "column2".getBytes());
        List<Cell> cells3 = put1.get("cf2".getBytes(), COLUMN.getBytes());
        List<Cell> cells4 = put2.get(FAMILY.getBytes(), COLUMN.getBytes());
        Cell cell1 = cells1.get(0);
        Cell cell2 = cells2.get(0);
        Cell cell3 = cells3.get(0);
        Cell cell4 = cells4.get(0);
        assertEquals(2, puts.size());
        assertEquals(1, cells1.size());
        assertEquals(1, cells2.size());
        assertEquals(1, cells3.size());
        assertEquals(1, cells4.size());
        assertEquals(ROW_KEY, Bytes.toString(put1.getRow()));
        assertEquals("val", Bytes.toString(CellUtil.cloneValue(cell1)));
        assertEquals("val2", Bytes.toString(CellUtil.cloneValue(cell2)));
        assertEquals("val3", Bytes.toString(CellUtil.cloneValue(cell3)));
        assertEquals("test", Bytes.toString(CellUtil.cloneValue(cell4)));
    }

    @Test
    public void testPutForOneInvalidRow() throws IOException {
        // GIVEN
        when(tablePool.getHTable(TABLE_NAME)).thenReturn(tableInterface);

        // WHEN
        hBaseOperations.put(TABLE_NAME, Arrays.asList(new HBaseRow()), true);

        // THEN
        verify(tableInterface).put(putCaptor.capture());
        List<Put> puts = putCaptor.getValue();
        assertEquals(0, puts.size());
    }

    @Test
    public void testPutForInvalidRowWithInvalidFamily() throws IOException {
        // GIVEN
        when(tablePool.getHTable(TABLE_NAME)).thenReturn(tableInterface);
        HBaseRow row1 = new HBaseRow(ROW_KEY, "", COLUMN, "val");
        HBaseRow row2 = new HBaseRow(ROW_KEY, null, COLUMN, "val");

        // WHEN
        hBaseOperations.put(TABLE_NAME, Arrays.asList(row1, row2), true);

        // THEN
        verify(tableInterface).put(putCaptor.capture());
        List<Put> puts = putCaptor.getValue();
        assertEquals(0, puts.size());
    }

    @Test
    public void testPutForInvalidRowWithInvalidColumn() throws IOException {
        // GIVEN
        when(tablePool.getHTable(TABLE_NAME)).thenReturn(tableInterface);
        HBaseRow row1 = new HBaseRow(ROW_KEY, FAMILY, "", "val");
        HBaseRow row2 = new HBaseRow(ROW_KEY, FAMILY, null, "val");

        // WHEN
        hBaseOperations.put(TABLE_NAME, Arrays.asList(row1, row2), true);

        // THEN
        verify(tableInterface).put(putCaptor.capture());
        List<Put> puts = putCaptor.getValue();
        assertEquals(0, puts.size());
    }

    @Test
    public void testPutForInvalidRowWithInvalidValue() throws IOException {
        // GIVEN
        when(tablePool.getHTable(TABLE_NAME)).thenReturn(tableInterface);
        HBaseRow row1 = new HBaseRow(ROW_KEY, FAMILY, COLUMN, "");
        HBaseRow row2 = new HBaseRow(ROW_KEY, FAMILY, COLUMN, null);

        // WHEN
        hBaseOperations.put(TABLE_NAME, Arrays.asList(row1, row2), true);

        // THEN
        verify(tableInterface).put(putCaptor.capture());
        List<Put> puts = putCaptor.getValue();
        assertEquals(0, puts.size());
    }

    @Test
    public void testGetForNullResult() throws IOException {
        // GIVEN
        when(tablePool.getHTable(TABLE_NAME)).thenReturn(tableInterface);
        when(tableInterface.get(any(Get.class))).thenReturn(null);

        // WHEN
        HBaseRow result = hBaseOperations.get(TABLE_NAME, ROW_KEY, true);

        // THEN
        assertNull(result);
    }

    @Test
    public void testGetForEmptyResult() throws IOException {
        // GIVEN
        Result mockResult = mock(Result.class);
        when(tablePool.getHTable(TABLE_NAME)).thenReturn(tableInterface);
        when(tableInterface.get(any(Get.class))).thenReturn(mockResult);
        when(mockResult.getMap()).thenReturn(null);

        // WHEN
        HBaseRow result = hBaseOperations.get(TABLE_NAME, ROW_KEY, true);

        // THEN
        assertNull(result);
    }

    @Test
    public void testGetForFullRow() throws IOException {
        // GIVEN
        Result mockResult = mock(Result.class);
        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> familyMap =
                new TreeMap<>(Bytes.BYTES_COMPARATOR);
        NavigableMap<byte[], NavigableMap<Long, byte[]>> columnMap =
                new TreeMap<>(Bytes.BYTES_COMPARATOR);
        NavigableMap<Long, byte[]> firstValue = new TreeMap<>();
        firstValue.put(System.currentTimeMillis(), "testValue".getBytes());
        NavigableMap<Long, byte[]> secondValue = new TreeMap<>();
        secondValue.put(System.currentTimeMillis(), "testValue123".getBytes());
        columnMap.put(COLUMN.getBytes(), firstValue);
        columnMap.put("column2".getBytes(), secondValue);
        familyMap.put(FAMILY.getBytes(), columnMap);
        when(tablePool.getHTable(TABLE_NAME)).thenReturn(tableInterface);
        when(tableInterface.get(any(Get.class))).thenReturn(mockResult);
        when(mockResult.getMap()).thenReturn(familyMap);
        when(mockResult.getRow()).thenReturn(ROW_KEY.getBytes());

        // WHEN
        HBaseRow result = hBaseOperations.get(TABLE_NAME, ROW_KEY, true);

        // THEN
        Map<Family, Map<Column, String>> families = result.getColumnFamilies();
        Map<Column, String> columnsMap = families.get(new Qualifier(FAMILY));
        String value1 = columnsMap.get(new Qualifier(COLUMN));
        String value2 = columnsMap.get(new Qualifier("column2"));
        assertEquals(ROW_KEY, result.getRowKey());
        assertEquals(1, families.size());
        assertEquals(2, columnsMap.size());
        assertEquals("testValue", value1);
        assertEquals("testValue123", value2);
    }

    @Test
    public void testGetForSpecificColumnFamily() throws IOException {
        // GIVEN
        when(tablePool.getHTable(TABLE_NAME)).thenReturn(tableInterface);
        when(tableInterface.get(any(Get.class))).thenReturn(null);

        // WHEN
        hBaseOperations.get(TABLE_NAME, ROW_KEY, FAMILY, new NullMapper(), true);

        // THEN
        verify(tableInterface).get(getCaptor.capture());
        Get get = getCaptor.getValue();
        assertEquals(ROW_KEY, Bytes.toString(get.getRow()));
        assertEquals(1, get.getFamilyMap().size());
        assertEquals(null, get.getFamilyMap().get(FAMILY.getBytes()));
        assertTrue(get.getFamilyMap().containsKey(FAMILY.getBytes()));
    }

    @Test
    public void testGetForSpecificColumnFamilyAndColumn() throws IOException {
        // GIVEN
        when(tablePool.getHTable(TABLE_NAME)).thenReturn(tableInterface);
        when(tableInterface.get(any(Get.class))).thenReturn(null);

        // WHEN
        hBaseOperations.get(TABLE_NAME, ROW_KEY, FAMILY, COLUMN, new NullMapper(), true);

        // THEN
        verify(tableInterface).get(getCaptor.capture());
        Get get = getCaptor.getValue();
        assertEquals(ROW_KEY, Bytes.toString(get.getRow()));
        assertEquals(1, get.getFamilyMap().size());
        assertEquals(COLUMN, Bytes.toString(get.getFamilyMap().get(FAMILY.getBytes()).last()));
        assertTrue(get.getFamilyMap().containsKey(FAMILY.getBytes()));
    }

    @Test
    public void testScanForSpecificFamilyAndColumn() throws IOException {
        // GIVEN
        ResultScanner resultScanner = mock(ResultScanner.class);
        when(tablePool.getHTable(TABLE_NAME)).thenReturn(tableInterface);
        when(tableInterface.getScanner(any(Scan.class))).thenReturn(resultScanner);

        // WHEN
        hBaseOperations.scan(TABLE_NAME, FAMILY, COLUMN, new NullExtractor(), true);

        // THEN
        verify(tableInterface).getScanner(scanCaptor.capture());
        verify(resultScanner).close();
        Scan scan = scanCaptor.getValue();
        assertTrue(scan.getFamilyMap().containsKey(FAMILY.getBytes()));
        assertEquals(1, scan.getFamilyMap().get(FAMILY.getBytes()).size());
        assertEquals(COLUMN, Bytes.toString(scan.getFamilyMap().get(FAMILY.getBytes()).last()));
    }

    @Test
    public void testExecuteForRelease() throws Exception {
        // GIVEN
        TableCallback callback = mock(TableCallback.class);
        when(tablePool.getHTable(TABLE_NAME)).thenReturn(tableInterface);
        when(callback.doInTable(tableInterface)).thenReturn(anyObject());

        // WHEN
        hBaseOperations.execute(TABLE_NAME, callback, true);

        // THEN
        verify(callback, times(1)).doInTable(tableInterface);
        verify(tablePool, times(1)).releaseHTable(TABLE_NAME);
    }

    @Test(expected = HBaseOperationException.class)
    public void testExecuteForReleaseException() throws Exception {
        // GIVEN
        TableCallback callback = mock(TableCallback.class);
        when(tablePool.getHTable(TABLE_NAME)).thenReturn(tableInterface);
        doThrow(new IOException()).when(tablePool).releaseHTable(TABLE_NAME);

        // WHEN
        hBaseOperations.execute(TABLE_NAME, callback, true);
    }

    @Test
    public void testExecuteForKeepAlive() throws Exception {
        // GIVEN
        TableCallback callback = mock(TableCallback.class);
        when(tablePool.getHTable(TABLE_NAME)).thenReturn(tableInterface);
        when(callback.doInTable(tableInterface)).thenReturn(anyObject());

        // WHEN
        hBaseOperations.execute(TABLE_NAME, callback, false);

        // THEN
        verify(callback, times(1)).doInTable(tableInterface);
        verify(tablePool, times(0)).releaseHTable(TABLE_NAME);
    }

    @Test(expected = HBaseOperationException.class)
    public void testExecuteForException() throws Exception {
        // GIVEN
        TableCallback callback = mock(TableCallback.class);
        when(tablePool.getHTable(TABLE_NAME)).thenReturn(tableInterface);
        when(callback.doInTable(tableInterface)).thenThrow(new Exception());

        // WHEN
        hBaseOperations.execute(TABLE_NAME, callback, false);

        // THEN
        verify(callback).doInTable(tableInterface);
    }

    private class NullMapper implements RowMapper<String> {
        @Override
        public String mapRow(Result result) throws Exception {
            return null;
        }
    }

    private class NullExtractor implements ResultExtractor<String> {
        @Override
        public String extractData(ResultScanner result) throws Exception {
            return null;
        }
    }

}
