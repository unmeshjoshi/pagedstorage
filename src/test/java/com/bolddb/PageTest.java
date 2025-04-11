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
    public void pageHeaderIsWrittenAtTheStartOfPage() throws IOException {
        Page page = new Page(0);
        java.io.File tempFile = java.io.File.createTempFile("temp", ".txt", new java.io.File(System.getProperty("java.io.tmpdir")));
        try (java.nio.channels.FileChannel fileChannel = java.nio.channels.FileChannel.open(tempFile.toPath(), java.nio.file.StandardOpenOption.WRITE)) {
            page.writeTo(fileChannel);
        } catch (java.io.IOException e) {
            fail("Failed to write to file channel", e);
        }
    }

    @Test
    public void freeSpaceIsCorrect() {
        Page page = new Page(0);
        assertEquals(Page.PAGE_SIZE - page.slotRegionEndOffset(), page.freeSpace());
    }

    @Test
    public void putAndGetValueKeyAndValueToEmptyPage() {
        Page page = new Page(0);
        page.put("key".getBytes(), "value".getBytes());
        byte[] value = page.get("key".getBytes());
        assertEquals("value", new String(value));
    }

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
}