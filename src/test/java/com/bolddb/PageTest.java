package com.bolddb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.junit.jupiter.api.Assertions.*;

class PageTest {

    @Test
    public void pageStartPosition() {
        assertEquals(0, new Page(0).pageStartPosition());
        assertEquals(Page.PAGE_SIZE, new Page(1).pageStartPosition());
        assertEquals(Page.PAGE_SIZE * 2, new Page(2).pageStartPosition());
        assertEquals(Page.PAGE_SIZE * 5, new Page(5).pageStartPosition());
    }

    @Test
    public void writePageToFile() throws IOException {
        Page page = new Page(1);
        java.io.File tempFile = java.io.File.createTempFile("temp", ".txt", new java.io.File(System.getProperty("java.io.tmpdir")));
        try (java.nio.channels.FileChannel fileChannel = java.nio.channels.FileChannel.open(tempFile.toPath(), java.nio.file.StandardOpenOption.WRITE)) {
            page.writeTo(fileChannel);
        } catch (java.io.IOException e) {
            fail("Failed to write to file channel", e);
        }
    }

    @Test
    public void writePageAtCorrectPosition(@TempDir Path tempDir) throws IOException {
        // Create a file for testing
        Path tempFile = tempDir.resolve("page_position_test.db");
        
        // Create pages with different IDs
        Page page1 = new Page(0); // First page (offset 0)
        Page page2 = new Page(1); // Second page (offset = PAGE_SIZE)
        Page page3 = new Page(2); // Third page (offset = 2 * PAGE_SIZE)
        
        // Write signature values at the beginning of each page
        // We need to put a value and ensure buffer position is reset before writing
        page1.writeInt(111);
        page2.writeInt(222);
        page3.writeInt(333);
        // Write pages to file - each page will position itself correctly
        try (FileChannel channel = FileChannel.open(tempFile, 
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ)) {
            
            // First ensure the file is large enough for all our pages
            Page.ensureFileSize(channel, page3.getPageId() + 1);
            
            // Write pages in any order - they should go to the correct positions
            page3.writeTo(channel); // Will be written at position 2*PAGE_SIZE
            page1.writeTo(channel); // Will be written at position 0
            page2.writeTo(channel); // Will be written at position PAGE_SIZE
            
            // File size should be at least as large as the highest page
            long expectedMinSize = (page3.getPageId() + 1) * Page.PAGE_SIZE;
            assertTrue(channel.size() >= expectedMinSize, 
                    "File size should be at least " + expectedMinSize + " bytes");
            
            // Now read the pages back and verify they're at the correct positions
            ByteBuffer readBuffer = ByteBuffer.allocate(Page.PAGE_SIZE);
            
            // Read page1 (at position 0)
            channel.position(0);
            readBuffer.clear();
            int bytesRead = channel.read(readBuffer);
            assertTrue(bytesRead > 0, "Should read data from page 1");
            assertEquals(111, readBuffer.getInt(0), "First page data should be 111");
            
            // Read page2 (at position PAGE_SIZE)
            channel.position(Page.PAGE_SIZE);
            readBuffer.clear();
            bytesRead = channel.read(readBuffer);
            assertTrue(bytesRead > 0, "Should read data from page 2");
            assertEquals(222, readBuffer.getInt(0), "Second page data should be 222");
            
            // Read page3 (at position 2*PAGE_SIZE)
            channel.position(2 * Page.PAGE_SIZE);
            readBuffer.clear();
            bytesRead = channel.read(readBuffer);
            assertTrue(bytesRead > 0, "Should read data from page 3");
            assertEquals(333, readBuffer.getInt(0), "Third page data should be 333");
        }
    }



    @Test
    public void pageShouldCalculateCorrectFilePosition() {
        // Test different page IDs
        assertEquals(0L, new Page(0).getPageId() * Page.PAGE_SIZE, "Page 0 should be at position 0");
        assertEquals(Page.PAGE_SIZE, new Page(1).getPageId() * Page.PAGE_SIZE, "Page 1 should be at position PAGE_SIZE");
        assertEquals(2L * Page.PAGE_SIZE, new Page(2).getPageId() * Page.PAGE_SIZE, "Page 2 should be at position 2*PAGE_SIZE");
        
        // Test a large page number to ensure no integer overflow
        long largePageId = 1000000; // 1 million
        Page largePage = new Page((int)largePageId);
        assertEquals(largePageId * Page.PAGE_SIZE, largePage.getPageId() * (long)Page.PAGE_SIZE, 
                "Large page ID should calculate correct position without overflow");
    }
    
    @Test
    public void ensureFileSizeShouldCreateLargeEnoughFile(@TempDir Path tempDir) throws IOException {
        Path tempFile = tempDir.resolve("file_size_test.db");
        
        try (FileChannel channel = FileChannel.open(tempFile, 
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ)) {
            
            // Initial file size should be 0
            assertEquals(0L, channel.size(), "New file should have size 0");
            
            // Ensure size for 1 page
            Page.ensureFileSize(channel, 1);
            assertEquals(Page.PAGE_SIZE, channel.size(), "File should be extended to 1 page");
            
            // Ensure size for 3 pages
            Page.ensureFileSize(channel, 3);
            assertEquals(3L * Page.PAGE_SIZE, channel.size(), "File should be extended to 3 pages");
            
            // Requesting smaller size should not change the file
            Page.ensureFileSize(channel, 2);
            assertEquals(3L * Page.PAGE_SIZE, channel.size(), "File size should not be reduced");
            
            // Ensure size for 100 pages
            Page.ensureFileSize(channel, 100);
            assertEquals(100L * Page.PAGE_SIZE, channel.size(), "File should be extended to 100 pages");
        }
    }
    
    @Test
    public void initShouldCreateAndInitializeFile(@TempDir Path tempDir) throws IOException {
        Path dbFile = tempDir.resolve("test_init.db");
        
        // Ensure file doesn't exist initially
        assertFalse(Files.exists(dbFile), "File should not exist before test");
        
        // Initialize with 10 pages
        int numPages = 10;
        try (FileChannel channel = Page.init(dbFile, numPages)) {
            // File should now exist
            assertTrue(Files.exists(dbFile), "File should exist after initialization");
            
            // File should have the correct size
            assertEquals(numPages * (long)Page.PAGE_SIZE, channel.size(), 
                    "File should be initialized to the correct size");
            
            // Test writing and reading a page
            Page testPage = new Page(5); // Page in the middle
            testPage.writeInt(12345);

            testPage.writeTo(channel);
            
            // Verify the page was written
            ByteBuffer readBuffer = ByteBuffer.allocate(Page.PAGE_SIZE);
            channel.position(5 * Page.PAGE_SIZE); // Position to read page 5
            channel.read(readBuffer);
            
            assertEquals(12345, readBuffer.getInt(0), "Should read the written value");
        }
        
        // Test reopening the file and extending it
        int newNumPages = 20;
        try (FileChannel channel = Page.init(dbFile, newNumPages)) {
            // File size should be extended
            assertEquals(newNumPages * (long)Page.PAGE_SIZE, channel.size(), 
                    "File should be extended to new size");
            
            // The original data should still be there
            ByteBuffer readBuffer = ByteBuffer.allocate(Page.PAGE_SIZE);
            channel.position(5 * Page.PAGE_SIZE); // Position to read page 5
            channel.read(readBuffer);
            
            assertEquals(12345, readBuffer.getInt(0), "Original data should be preserved");
        }
    }
}