package com.bolddb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

public class TableTest {

    @Test
    public void testSerializationAndDeserialization() throws IOException {
        // Create a row with various attributes
        Row row = new Row("customer1".getBytes());
        row.setAttribute("name", "John Doe");
        row.setAttribute("age", 30);
        row.setAttribute("isActive", true);
        row.setAttribute("balance", 1250.75);
        
        // Serialize
        byte[] serialized = row.serialize();
        
        // Deserialize
        Row deserialized = Row.deserialize(serialized);
        
        // Verify
        assertArrayEquals("customer1".getBytes(), deserialized.getPrimaryKey());
        assertEquals("John Doe", deserialized.getAttribute("name"));
        assertEquals(30, deserialized.getAttribute("age"));
        assertEquals(true, deserialized.getAttribute("isActive"));
        assertEquals(1250.75, deserialized.getAttribute("balance"));
    }

    @Test
    public void testTableBasicOperations(@TempDir Path tempDir) throws IOException {
        // Create a table
        Table customerTable = new Table("customer", tempDir);
        
        // Create a few customer rows
        Row customer1 = new Row("1001".getBytes());
        customer1.setAttribute("name", "John Doe");
        customer1.setAttribute("email", "john@example.com");
        customer1.setAttribute("age", 30);
        
        Row customer2 = new Row("1002".getBytes());
        customer2.setAttribute("name", "Jane Smith");
        customer2.setAttribute("email", "jane@example.com");
        customer2.setAttribute("age", 25);
        
        // Add rows to the table
        assertTrue(customerTable.insert(customer1));
        assertTrue(customerTable.insert(customer2));
        assertEquals(2, customerTable.size());
        
        // Retrieve rows
        Row retrievedCustomer1 = customerTable.get("1001".getBytes()); // This will need to be updated if get expects byte[]
        assertNotNull(retrievedCustomer1);
        assertEquals("John Doe", retrievedCustomer1.getAttribute("name"));
        assertEquals("john@example.com", retrievedCustomer1.getAttribute("email"));
        assertEquals(30, retrievedCustomer1.getAttribute("age"));
        
        Row retrievedCustomer2 = customerTable.get("1002".getBytes());
        assertNotNull(retrievedCustomer2);
        assertEquals("Jane Smith", retrievedCustomer2.getAttribute("name"));
    }

    @Test
    public void testTableWithMultiplePages(@TempDir Path tempDir) throws IOException {
        // Create a table in the temp directory
        Table table = new Table("multi_page_test", tempDir);
        
        // Calculate how many rows we need to fill one page
        int rowsPerPage = Page.PAGE_SIZE / 2000;
        
        // Insert enough rows to require 3 pages
        int totalRowsToInsert = rowsPerPage * 3;
        
        // Insert the rows
        for (int i = 0; i < totalRowsToInsert; i++) {
            String key = "key" + i;
            Row row = new Row(key.getBytes());
            
            // Create a value that takes up about 1500 bytes
            String largeValue = generateString(1500);
            row.setAttribute("data", largeValue);
            
            // Insert should succeed
            assertTrue(table.insert(row), "Insert should succeed for row " + i);
        }
        
        // Save the table
        table.save();
        
        // Verify we can read back all the rows
        for (int i = 0; i < totalRowsToInsert; i++) {
            String key = "key" + i;
            Row retrievedRow = table.get(key.getBytes());
            assertNotNull(retrievedRow, "Should retrieve row with key: " + key);
            assertArrayEquals(key.getBytes(), retrievedRow.getPrimaryKey(), "Retrieved row should have correct key");
        }
        
        // Verify the total number of rows matches what we inserted
        assertEquals(totalRowsToInsert, table.size(), "Table size should match number of inserted rows");
    }

    @Test
    public void testTableExceedingPageCapacity(@TempDir Path tempDir) throws IOException {
        // Create a table
        Table largeDataTable = new Table("large_data", tempDir);
        
        // Keep track of inserted rows
        int insertedRows = 0;
        int totalDataSize = 0;
        
        // Insert enough rows to require multiple pages
        int targetRows = 100; // This should require multiple pages
        
        for (int i = 0; i < targetRows; i++) {
            String key = "key" + i;
            
            // Create a large value - increase size for each insert
            // Make sure value size + key size + slot overhead doesn't exceed page size
            // We need to account for:
            // - Key size (5 bytes)
            // - Slot overhead (8 bytes)
            // - Page header (16 bytes)
            // - Extra space for internal data structures
            int maxValueSize = 3500; // Keep well below page size to ensure it fits
            int valueSize = Math.min(200 + (i * 50), maxValueSize); // Start with 200 bytes, increase by 50 each time
            String largeValue = generateString(valueSize);
            
            Row row = new Row(key.getBytes());
            row.setAttribute("data", largeValue);
            
            // Insert should succeed
            assertTrue(largeDataTable.insert(row), "Insert should succeed for row " + i);
            
            // Keep track of how many we've inserted and the total size
            insertedRows++;
            totalDataSize += key.getBytes().length + valueSize;
            
            System.out.println("Inserted row " + insertedRows + 
                    " with value size " + valueSize + 
                    " bytes, total so far: " + totalDataSize + " bytes");
        }
        
        // Save the table
        largeDataTable.save();
        
        // Verify we inserted all rows
        assertEquals(targetRows, insertedRows, "Should insert all target rows");
        
        // Verify we can read back all the rows
        for (int i = 0; i < insertedRows; i++) {
            String key = "key" + i;
            Row retrievedRow = largeDataTable.get(key.getBytes());
            assertNotNull(retrievedRow, "Should retrieve row with key: " + key);
        }
        
        // Verify the total number of rows matches what we inserted
        assertEquals(targetRows, largeDataTable.size(), "Table size should match number of inserted rows");
    }

    /**
     * Helper method to generate a string of the specified size.
     */
    private String generateString(int size) {
        StringBuilder sb = new StringBuilder(size);
        for (int i = 0; i < size; i++) {
            // Use characters in the ASCII range 32-126 (printable characters)
            sb.append((char) (32 + (i % 95)));
        }
        return sb.toString();
    }
} 