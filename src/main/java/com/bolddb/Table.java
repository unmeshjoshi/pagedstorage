package com.bolddb;

import java.io.IOException;
import java.nio.file.Path;


import java.nio.file.StandardOpenOption;
import java.nio.channels.FileChannel;

/**
 * A simple table implementation that stores rows in memory and uses a single Page
 * for persistence.
 */
public class Table {
    private final String name;
    private final Path tablePath;
    private int totalPages; // Total number of pages in the table
    // Holds only modified pages
    private final java.util.Map<Integer, Page> dirtyPages = new java.util.HashMap<>();
    
    /**
     * Creates a new table with the specified name in the given directory.
     * The table data will be stored in a file named {tableName}.table
     * 
     * @param name The name of the table
     * @param baseDirectory The directory where the table file will be stored
     * @throws IOException If an I/O error occurs
     */
    public Table(String name, Path baseDirectory) throws IOException {
        this.name = name;
        this.tablePath = baseDirectory.resolve(name + ".table");
        java.nio.file.Files.createDirectories(baseDirectory);

        if (!java.nio.file.Files.exists(tablePath)) {
            setupFirstPageInMemory();
            return;
        }
        this.totalPages = getTotalPagesFromFile();
        if (this.totalPages == 0) {
            setupFirstPageInMemory();
        }
    }

    /**
     * Returns the total number of pages in the table file based on its size.
     */
    private int getTotalPagesFromFile() throws IOException {
        long fileSize = java.nio.file.Files.size(tablePath);
        return (fileSize > 0) ? (int) (fileSize / Page.PAGE_SIZE) : 0;
    }

    /**
     * Prepares the first page in memory for a new or empty table,
     * marks it as dirty, and sets totalPages to 1.
     */
    private void setupFirstPageInMemory() {
        assert totalPages == 0 : "setupFirstPageInMemory should only be called when there are no pages";
        Page firstPage = new Page(0);
        dirtyPages.put(0, firstPage);
        totalPages = 1;
    }

    public String getName() {
        return name;
    }

    /**
     * Gets a page from the cache or loads it from disk if not present.
     * 
     * @param pageId The ID of the page to get
     * @return The requested page
     * @throws IOException If an I/O error occurs
     */
    private Page getPage(int pageId) throws IOException {
        System.out.println("[DEBUG] getPage called for pageId: " + pageId);
        if (pageId < 0 || pageId >= totalPages) {
            throw new IOException("Invalid pageId: " + pageId + " (totalPages=" + totalPages + ")");
        }
        // Track page access for benchmarking

        // Check if page is dirty (modified but not yet saved)
        Page page = dirtyPages.get(pageId);
        if (page != null) {
            return page;
        }
        // Otherwise, read directly from file
        try (FileChannel channel = FileChannel.open(tablePath, StandardOpenOption.READ)) {
            Page.ensurePageExists(channel, pageId);
            page = new Page(pageId);
            page.readFrom(channel);
            return page;
        }
    }

    /**
     * Inserts a row into the table.
     * 
     * @param row The row to insert
     * @return true if the row was inserted successfully, false if there wasn't enough space
     * @throws IOException If an I/O error occurs
     */
    public boolean insert(Row row) throws IOException {
        logInsertAttempt(row);
        byte[] keyBytes = serializeKey(row);
        byte[] valueBytes = serializeValue(row);
        Page currentPage = findOrCreatePage(keyBytes, valueBytes);
        boolean success = currentPage.put(keyBytes, valueBytes);
        if (success) {
            markPageDirty(currentPage);
        }
        logInsertResult(success);
        return success;
    }

    // --- Helper methods for insert ---
    private void logInsertAttempt(Row row) {
        System.out.println("Inserting row with key: " + java.util.Arrays.toString(row.getPrimaryKey()));
    }

    private byte[] serializeKey(Row row) {
        return row.getPrimaryKey();
    }

    private byte[] serializeValue(Row row) {
        byte[] valueBytes = row.serialize();
        System.out.println("Serialized row size - Key: " + row.getPrimaryKey().length +
                " bytes, Value: " + valueBytes.length + " bytes");
        return valueBytes;
    }

    private Page findOrCreatePage(byte[] keyBytes, byte[] valueBytes) throws IOException {
        assert totalPages > 0 : "There must be at least one page in the table";
        int requiredSpace = keyBytes.length + valueBytes.length + 8; // 8 for slot
        Page currentPage = getPage(totalPages - 1); // Get the last page
        System.out.println("Page free space: " + currentPage.freeSpace() + " bytes");
        if (requiredSpace > currentPage.freeSpace()) {
            System.out.println("Not enough space in current page. Flushing full page to disk and creating new page.");
            savePage(currentPage);
            dirtyPages.clear(); // Only keep the new page in memory
            System.out.println("Creating new page.");
            Page newPage = new Page(totalPages);
            dirtyPages.put(totalPages, newPage);
            totalPages++;
            return newPage;
        }
        return currentPage;
    }

    private void markPageDirty(Page page) {
        dirtyPages.put((int) page.header.pageId(), page);
    }

    private void logInsertResult(boolean success) {
        System.out.println("Insert " + (success ? "succeeded" : "failed"));
    }

    public static class GetResult {
        public final Row row;
        public final int pagesAccessed;
        public GetResult(Row row, int pagesAccessed) {
            this.row = row;
            this.pagesAccessed = pagesAccessed;
        }
    }

    public GetResult get(byte[] primaryKey) throws IOException {
        int pagesAccessed = 0;
        System.out.println("Getting row with key: " + java.util.Arrays.toString(primaryKey));
        for (int i = 0; i < totalPages; i++) {
            pagesAccessed++;
            System.out.println("[DEBUG] Accessing page ID: " + i);
            Page page = getPage(i);
            byte[] valueBytes = page.get(primaryKey);
            if (valueBytes != null) {
                System.out.println("[DEBUG] Key found in page ID: " + i);
                return new GetResult(Row.deserialize(valueBytes), pagesAccessed);
            }
        }
        System.out.println("[DEBUG] Key not found in any page");
        return new GetResult(null, pagesAccessed);
    }

    public int size() {
        int total = 0;
        for (int i = 0; i < totalPages; i++) {
            try {
                total += getPage(i).count();
            } catch (IOException e) {
                // Log error but continue
                System.err.println("Error getting page " + i + ": " + e.getMessage());
            }
        }
        return total;
    } // This logic is unchanged, but now getPage reads directly from file or dirty set.

    /**
     * Saves all pages to the file.
     * 
     * @throws IOException If an I/O error occurs
     */
    public void save() throws IOException {
        try (FileChannel channel = FileChannel.open(tablePath,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE)) {
            for (Integer i : dirtyPages.keySet()) {
                Page page = dirtyPages.get(i);
                page.writeTo(channel);
            }
            System.out.println("Total pages written = " + totalPages);
            dirtyPages.clear(); // Only after all pages are written
        }
    }
    // Immediately flushes a single page to disk at its correct offset.
    private void savePage(Page page) throws IOException {
        try (FileChannel channel = FileChannel.open(tablePath, StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
            page.writeTo(channel);
        }
    }

    /**
     * Returns the total number of pages in the table.
     */
    public int getTotalPages() {
        return totalPages;
    }
}