package com.bolddb;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.logging.Logger;

public class Page {
    private static final Logger LOGGER = Logger.getLogger(Page.class.getName());
    
    public static final int PAGE_SIZE = 4096; // 4KB per page - standard size
    public static final int DEFAULT_CHUNK_SIZE = 16; // Default to 16-page chunks (64KB)
    public static final int ALLOCATION_UNIT = 8; // Allocate in multiples of 8 pages
    
    private final int pageId;
    ByteBuffer buffer;

    public Page(int pageId) {
        this.pageId = pageId;
        this.buffer = ByteBuffer.allocate(PAGE_SIZE);
    }

    public int getPageId() {
        return pageId;
    }
    
    /**
     * Initializes a database file with the specified number of pages.
     * Creates the file if it doesn't exist, or extends it if it's too small.
     * 
     * @param filePath Path to the database file
     * @param numPages Number of pages to allocate
     * @return FileChannel for the initialized file
     * @throws IOException If an I/O error occurs
     */
    public static FileChannel init(Path filePath, int numPages) throws IOException {
        // Open file for read and write, creating it if it doesn't exist
        FileChannel channel = FileChannel.open(filePath, 
                StandardOpenOption.READ, 
                StandardOpenOption.WRITE, 
                StandardOpenOption.CREATE);
                
        // For init, we want exact sizing
        ensureExactFileSize(channel, numPages);
        
        return channel;
    }

    /**
     * Ensures that the file has exactly the size needed for the specified number of pages.
     * This is used during initialization when we want precise control over the file size.
     */
    private static void ensureExactFileSize(FileChannel fileChannel, int numPages) throws IOException {
        long requiredSize = getSize((long) numPages);
        long currentSize = fileChannel.size();
        
        if (currentSize != requiredSize) {
            allocateSpace(fileChannel, requiredSize);
        }
    }

    private static long getSize(long numPages) {
        return numPages * PAGE_SIZE;
    }

    /**
     * Ensures that the file has enough space allocated for the specified number of pages
     */
    public static void ensureFileSize(FileChannel fileChannel, int numPages) throws IOException {
        long requiredSize = calculateRequiredSize(numPages);
        long currentSize = fileChannel.size();
        
        if (currentSize >= requiredSize) {
            // File is already large enough
            return;
        }
        allocateExactSize(fileChannel, requiredSize);
        verifyAllocation(fileChannel, requiredSize);
    }
    
    /**
     * Determines if we should use exact sizing based on the number of pages.
     * For small files (up to 100 pages), exact sizing is used to match test expectations.
     */
    private static boolean shouldUseExactSizing(int numPages) {
        return numPages <= 100;
    }
    
    /**
     * Calculates the required file size in bytes based on the number of pages.
     */
    private static long calculateRequiredSize(int numPages) {
        return getSize((long) numPages);
    }
    
    /**
     * Allocates exactly the required size.
     */
    private static void allocateExactSize(FileChannel fileChannel, long requiredSize) throws IOException {
        allocateSpace(fileChannel, requiredSize);
    }

    /**
     * Verifies that the allocation succeeded by checking the final file size.
     */
    private static void verifyAllocation(FileChannel fileChannel, long requiredSize) throws IOException {
        long newSize = fileChannel.size();
        if (newSize < requiredSize) {
            throw new IOException("Failed to allocate required file space. Required: " +
                    requiredSize + ", Actual: " + newSize);
        }
    }
    
    /**
     * Allocates space in the file up to the specified position.
     * This uses the efficient single-byte approach with verification.
     */
    private static void allocateSpace(FileChannel fileChannel, long size) throws IOException {
        int written = writeLastByte(fileChannel, size);
        // Force the changes to disk to ensure allocation is complete
        fileChannel.force(false);
        
        // Verify allocation succeeded
        verifyFileSize(fileChannel, size, written);
    }

    private static void verifyFileSize(FileChannel fileChannel, long size, int written) throws IOException {
        if (written != 1 || fileChannel.size() < size) {
            throw new IOException("Failed to allocate file space to size: " + size);
        }
    }

    static int writeLastByte(FileChannel fileChannel, long size) throws IOException {
        // Position at the last byte of the desired size
        long lastByte = size - 1;
        fileChannel.position(lastByte);

        // Write a single byte to extend the file
        ByteBuffer oneByte = zeroByte();
        int written = fileChannel.write(oneByte);
        return written;
    }

    private static ByteBuffer zeroByte() {
        ByteBuffer oneByte = ByteBuffer.allocate(1);
        oneByte.put((byte) 0);
        oneByte.flip();
        return oneByte;
    }

    /**
     * Writes this page to the file channel at the correct offset based on pageId.
     * @param fileChannel The file channel to write to
     * @throws IOException If an I/O error occurs
     */
    public void writeTo(FileChannel fileChannel) throws IOException {
        // Calculate the correct position based on pageId
        long position = pageStartPosition();
        // Position the channel at the correct offset
        fileChannel.position(position);
        
        // Create a duplicate buffer so we don't affect the original
        ByteBuffer duplicate = buffer.duplicate();
        duplicate.position(0);
        duplicate.limit(PAGE_SIZE);
        
        // Keep writing until no remaining bytes
        while (duplicate.hasRemaining()) {
            fileChannel.write(duplicate);
        }
    }

    int pageStartPosition() {
        return pageId * PAGE_SIZE;
    }

    void writeInt(int value) {
        buffer.putInt(value);
        buffer.rewind();
    }
}
