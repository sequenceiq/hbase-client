package com.sequenceiq.hbase.admin;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class HBaseManagerTest {

    private static final String TABLE_NAME = "test_table";
    private HBaseManager hBaseManager;

    @Mock
    private HBaseAdmin hBaseAdmin;
    @Captor
    private ArgumentCaptor<HTableDescriptor> descriptorCaptor;

    @Before
    public void reset() throws IOException {
        hBaseManager = new HBaseManager(hBaseAdmin);
    }

    @Test
    public void testIsAvailableForNonExistingTable() throws IOException {
        // GIVEN
        when(hBaseAdmin.isTableAvailable(TABLE_NAME)).thenReturn(false);

        // WHEN
        boolean result = hBaseManager.isAvailable(TABLE_NAME);

        // THEN
        assertFalse(result);
    }

    @Test
    public void testIsAvailableForExistingDisabledTable() throws IOException {
        // GIVEN
        when(hBaseAdmin.isTableAvailable(TABLE_NAME)).thenReturn(true);
        when(hBaseAdmin.isTableEnabled(TABLE_NAME)).thenReturn(false);

        // WHEN
        boolean result = hBaseManager.isAvailable(TABLE_NAME);

        // THEN
        assertFalse(result);
    }

    @Test
    public void testIsAvailableForExistingEnabledTable() throws IOException {
        // GIVEN
        when(hBaseAdmin.isTableAvailable(TABLE_NAME)).thenReturn(true);
        when(hBaseAdmin.isTableEnabled(TABLE_NAME)).thenReturn(true);

        // WHEN
        boolean result = hBaseManager.isAvailable(TABLE_NAME);

        // THEN
        assertTrue(result);
    }

    @Test
    public void testIsAvailableForException() throws IOException {
        // GIVEN
        when(hBaseAdmin.isTableAvailable(TABLE_NAME)).thenThrow(new IOException());

        // WHEN
        boolean result = hBaseManager.isAvailable(TABLE_NAME);

        // THEN
        assertFalse(result);
    }

    @Test
    public void testCreateWithoutColumnFamilies() throws IOException {
        // WHEN
        boolean result = hBaseManager.create(TABLE_NAME);

        // THEN
        verify(hBaseAdmin).createTable(descriptorCaptor.capture());
        HTableDescriptor tableDescriptor = descriptorCaptor.getValue();
        assertTrue(result);
        assertEquals(TABLE_NAME, tableDescriptor.getNameAsString());
        assertEquals(0, tableDescriptor.getColumnFamilies().length);
    }

    @Test
    public void testCreateWithColumnFamilies() throws IOException {
        // WHEN
        boolean result = hBaseManager.create(TABLE_NAME, "cf1", "cf2");

        // THEN
        verify(hBaseAdmin).createTable(descriptorCaptor.capture());
        HTableDescriptor tableDescriptor = descriptorCaptor.getValue();
        assertTrue(result);
        assertEquals(TABLE_NAME, tableDescriptor.getNameAsString());
        assertEquals(2, tableDescriptor.getColumnFamilies().length);
    }

    @Test
    public void testCreateForException() throws IOException {
        // GIVEN
        doThrow(new MasterNotRunningException()).when(hBaseAdmin).createTable(any(HTableDescriptor.class));

        // WHEN
        boolean result = hBaseManager.create(TABLE_NAME, "cf1", "cf2");

        // THEN
        verify(hBaseAdmin).createTable(descriptorCaptor.capture());
        HTableDescriptor tableDescriptor = descriptorCaptor.getValue();
        assertFalse(result);
        assertEquals(TABLE_NAME, tableDescriptor.getNameAsString());
        assertEquals(2, tableDescriptor.getColumnFamilies().length);
    }

    @Test
    public void testCreateForNull() throws IOException {
        // WHEN
        boolean result = hBaseManager.create(TABLE_NAME, null);

        // THEN
        verify(hBaseAdmin).createTable(descriptorCaptor.capture());
        HTableDescriptor tableDescriptor = descriptorCaptor.getValue();
        assertTrue(result);
        assertEquals(TABLE_NAME, tableDescriptor.getNameAsString());
        assertEquals(0, tableDescriptor.getColumnFamilies().length);
    }

    @Test
    public void testDeleteIfCannotDisable() throws IOException {
        // GIVEN
        doThrow(new IOException()).when(hBaseAdmin).disableTable(TABLE_NAME.getBytes());
        when(hBaseAdmin.isTableEnabled(TABLE_NAME)).thenReturn(true);

        // WHEN
        boolean result = hBaseManager.delete(TABLE_NAME);

        // THEN
        assertFalse(result);
    }

    @Test
    public void testDeleteForException() throws IOException {
        // GIVEN
        doThrow(new IOException()).when(hBaseAdmin).deleteTable(TABLE_NAME.getBytes());

        // WHEN
        boolean result = hBaseManager.delete(TABLE_NAME);

        // THEN
        assertFalse(result);
    }

    @Test
    public void testDeleteForDisabledTable() throws IOException {
        // GIVEN
        when(hBaseAdmin.isTableEnabled(TABLE_NAME)).thenReturn(false);

        // WHEN
        boolean result = hBaseManager.delete(TABLE_NAME);

        // THEN
        assertTrue(result);
    }

    @Test
    public void testIsEnabledForDisabledTable() throws IOException {
        // GIVEN
        when(hBaseAdmin.isTableEnabled(TABLE_NAME)).thenReturn(false);

        // WHEN
        boolean result = hBaseManager.isEnabled(TABLE_NAME);

        // THEN
        assertFalse(result);
    }

    @Test
    public void testIsEnabledForEnabledTable() throws IOException {
        // GIVEN
        when(hBaseAdmin.isTableEnabled(TABLE_NAME)).thenReturn(true);

        // WHEN
        boolean result = hBaseManager.isEnabled(TABLE_NAME);

        // THEN
        assertTrue(result);
    }

    @Test
    public void testIsEnabledForException() throws IOException {
        // GIVEN
        when(hBaseAdmin.isTableEnabled(TABLE_NAME)).thenThrow(new IOException());

        // WHEN
        boolean result = hBaseManager.isEnabled(TABLE_NAME);

        // THEN
        assertFalse(result);
    }

    @Test
    public void testDisableForException() throws IOException {
        // GIVEN
        doThrow(new IOException()).when(hBaseAdmin).disableTable(TABLE_NAME.getBytes());

        // WHEN
        boolean result = hBaseManager.disable(TABLE_NAME);

        // THEN
        assertFalse(result);
    }

    @Test
    public void testDisable() throws IOException {
        // WHEN
        boolean result = hBaseManager.disable(TABLE_NAME);

        // THEN
        assertTrue(result);
    }


}
