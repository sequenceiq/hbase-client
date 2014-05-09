package com.sequenceiq.hbase.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

public class HBaseRowTest {

    private static final String FAM = "family";
    private static final String COL = "column";
    private static final Qualifier FAMILY = new Qualifier(FAM);
    private static final Qualifier COLUMN = new Qualifier(COL);
    private HBaseRow hBaseRow;

    @Before
    public void reset() {
        hBaseRow = new HBaseRow();
    }

    @Test
    public void testAddValueForNullMap() {
        // WHEN
        hBaseRow.addValue(FAM, COL, "test");

        // THEN
        Map<Family, Map<Column, String>> columnFamilies = hBaseRow.getColumnFamilies();
        assertEquals(1, columnFamilies.size());
        assertTrue(columnFamilies.containsKey(FAMILY));
        assertEquals(1, columnFamilies.get(FAMILY).size());
        assertEquals("test", columnFamilies.get(FAMILY).get(COLUMN));
    }

    @Test
    public void testAddValueForExistingValues() {
        // GIVEN
        Map<Family, Map<Column, String>> families = new HashMap<>();
        Map<Column, String> values = new HashMap<>();
        values.put(COLUMN, "test");
        families.put(FAMILY, values);
        Family family2 = new Qualifier("family2");
        Column column2 = new Qualifier("column2");

        // WHEN
        hBaseRow.setColumnFamilies(families);
        hBaseRow.addValue("family2", "column2", "test2");

        // THEN
        Map<Family, Map<Column, String>> columnFamilies = hBaseRow.getColumnFamilies();
        assertEquals(2, columnFamilies.size());
        assertTrue(columnFamilies.get(family2).containsKey(column2));
        assertEquals("test2", columnFamilies.get(family2).get(column2));
    }

    @Test
    public void testAddValueOverrideExistingValue() {
        // GIVEN
        Map<Family, Map<Column, String>> families = new HashMap<>();
        Map<Column, String> values = new HashMap<>();
        values.put(COLUMN, "test");
        families.put(FAMILY, values);
        hBaseRow.setColumnFamilies(families);

        // WHEN
        hBaseRow.addValue(FAM, COL, "test2");

        // THEN
        Map<Family, Map<Column, String>> columnFamilies = hBaseRow.getColumnFamilies();
        assertEquals(1, columnFamilies.size());
        assertEquals("test2", columnFamilies.get(FAMILY).get(COLUMN));
    }

}
