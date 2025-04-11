package com.bolddb;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A simple table implementation that stores rows in memory and uses a single Page
 * for persistence.
 */
public class Table {
    private final String name;
    private final Page page;           // Single page for storage
    
    /**
     * Creates a new table with the specified name and file path.
     * 
     * @param name The name of the table
     * @throws IOException If an I/O error occurs
     */
    public Table(String name) throws IOException {
        this.name = name;

        
        // Create a page for the table regardless of whether file exists
        this.page = new Page(0);

    }

    public String getName() {
        return name;
    }

    /**
     * Inserts a row into the table.
     * 
     * @param row The row to insert
     * @return true if the row was inserted successfully, false if there wasn't enough space
     * @throws IOException If an I/O error occurs
     */
    public boolean insert(Row row) throws IOException {
        System.out.println("Inserting row with key: " + row.getPrimaryKey());
        
        // Serialize the row and store in page
        byte[] keyBytes = row.getPrimaryKey().getBytes();
        byte[] valueBytes = row.serialize();
        
        System.out.println("Serialized row size - Key: " + keyBytes.length + 
                " bytes, Value: " + valueBytes.length + " bytes");
        System.out.println("Page free space: " + page.freeSpace() + " bytes");
        
        // Check if there's enough space
        int requiredSpace = keyBytes.length + valueBytes.length + 8; // 8 for slot
        if (requiredSpace > page.freeSpace()) {
            System.out.println("Not enough space in page. Required: " + requiredSpace + 
                    ", Available: " + page.freeSpace());
            return false; // Not enough space to insert
        }
        
        // Insert into the page
        boolean success = page.put(keyBytes, valueBytes);
        System.out.println("Insert " + (success ? "succeeded" : "failed"));

        return success;
    }

    public Row get(String primaryKey) throws IOException {
        System.out.println("Getting row with key: " + primaryKey);
        
        // If not in memory, try to find in the page
        byte[] keyBytes = primaryKey.getBytes();
        byte[] valueBytes = page.get(keyBytes);
        
        if (valueBytes != null) {
            System.out.println("Found row in page. Value size: " + valueBytes.length + " bytes");
            Row loadedRow = Row.deserialize(valueBytes);
            return loadedRow;
        }
        
        System.out.println("Row not found in page");
        return null;  // Not found
    }

    public int size() {
        return page.count();
    }

}