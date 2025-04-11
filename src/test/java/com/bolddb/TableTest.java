package com.bolddb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class TableTest {

    @Test
    public void testSerializationAndDeserialization() throws IOException {
        // Create a row with various attributes
        Row row = new Row("customer1");
        row.setAttribute("name", "John Doe");
        row.setAttribute("age", 30);
        row.setAttribute("isActive", true);
        row.setAttribute("balance", 1250.75);
        
        // Serialize
        byte[] serialized = row.serialize();
        
        // Deserialize
        Row deserialized = Row.deserialize(serialized);
        
        // Verify
        assertEquals("customer1", deserialized.getPrimaryKey());
        assertEquals("John Doe", deserialized.getAttribute("name"));
        assertEquals(30, deserialized.getAttribute("age"));
        assertEquals(true, deserialized.getAttribute("isActive"));
        assertEquals(1250.75, deserialized.getAttribute("balance"));
    }

    @Test
    public void testTableBasicOperations(@TempDir Path tempDir) throws IOException {
        // Create a table
        Table customerTable = new Table("customer");
        
        // Create a few customer rows
        Row customer1 = new Row("1001");
        customer1.setAttribute("name", "John Doe");
        customer1.setAttribute("email", "john@example.com");
        customer1.setAttribute("age", 30);
        
        Row customer2 = new Row("1002");
        customer2.setAttribute("name", "Jane Smith");
        customer2.setAttribute("email", "jane@example.com");
        customer2.setAttribute("age", 25);
        
        // Add rows to the table
        assertTrue(customerTable.insert(customer1));
        assertTrue(customerTable.insert(customer2));
        assertEquals(2, customerTable.size());
        
        // Retrieve rows
        Row retrievedCustomer1 = customerTable.get("1001");
        assertNotNull(retrievedCustomer1);
        assertEquals("John Doe", retrievedCustomer1.getAttribute("name"));
        assertEquals("john@example.com", retrievedCustomer1.getAttribute("email"));
        assertEquals(30, retrievedCustomer1.getAttribute("age"));
        
        Row retrievedCustomer2 = customerTable.get("1002");
        assertNotNull(retrievedCustomer2);
        assertEquals("Jane Smith", retrievedCustomer2.getAttribute("name"));
    }

    @Test
    public void testTableExceedingPageCapacity(@TempDir Path tempDir) throws IOException {
        // Create a table with a single page
        Table largeDataTable = new Table("large_data");
        
        // Keep track of inserted rows
        int insertedRows = 0;
        int totalDataSize = 0;
        
        try {
            // Generate data until we exceed page capacity
            while (true) {
                // Generate a row with increasingly larger values
                String key = "key" + insertedRows;
                
                // Create a large value - increase size for each insert
                // We'll use a base size plus an increasing component
                int valueSize = 200 + (insertedRows * 50); // Start with 200 bytes, increase by 50 each time
                String largeValue = generateString(valueSize);
                
                Row row = new Row(key);
                row.setAttribute("data", largeValue);
                
                // Try to insert - this should eventually fail
                boolean insertSucceeded = largeDataTable.insert(row);
                
                // If insert failed, we've reached capacity
                if (!insertSucceeded) {
                    System.out.println("Insert failed after " + insertedRows + " rows");
                    System.out.println("Total data size attempted: " + totalDataSize + " bytes");
                    break;
                }
                
                // Keep track of how many we've inserted and the total size
                insertedRows++;
                totalDataSize += key.getBytes().length + valueSize;
                
                System.out.println("Inserted row " + insertedRows + 
                        " with value size " + valueSize + 
                        " bytes, total so far: " + totalDataSize + " bytes");
            }
            
            // Verify we inserted at least some rows
            assertTrue(insertedRows > 0, "Should insert at least one row");
            
            // Verify we can read back all the rows we inserted
            for (int i = 0; i < insertedRows; i++) {
                String key = "key" + i;
                Row retrievedRow = largeDataTable.get(key);
                assertNotNull(retrievedRow, "Should retrieve row with key: " + key);
            }
            
            // Verify that our total data fits within expected bounds
            // Allow for some overhead for slots, keys, etc.
            assertTrue(totalDataSize < Page.PAGE_SIZE, 
                    "Total data size should be less than page size");
            assertTrue(totalDataSize > Page.PAGE_SIZE / 2, 
                    "Should utilize at least half of page capacity");
            
            System.out.println("Final stats: " + insertedRows + " rows inserted, " +
                    totalDataSize + " bytes of data stored");
        } finally {
          
        }
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