package com.bolddb;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.logging.Logger;

/**
 *                           PAGE (4096 bytes)
 * +----------------+----------------+----------------+----------------+
 * |                |                |                |                |
 * |   PageHeader   |   SlotArray    |   FreeSpace    |   DataArea     |
 * |   (16 bytes)   |   (16 bytes)   |                |                |
 * |                |                |                |                |
 * +----------------+----------------+----------------+----------------+
 * ↓                ↓                                 ↑                ↑
 * 0                16                                4050             4096
 *
 *
 * PageHeader (16 bytes):
 * +------------+--------+-------+----------+
 * | Page ID    | Flags  | Count | Overflow |
 * | (8 bytes)  |(2 bytes)|(2 bytes)|(4 bytes)|
 * +------------+--------+-------+----------+
 * | 0x00000000 | 0x0002 | 0x0002 | 0x000000 |
 * +------------+--------+-------+----------+
 *   Page #0      LEAF     2 entries  No overflow
 *
 *
 * SlotArray (16 bytes for 2 entries):
 * +--------------+----------+------------+   +--------------+----------+------------+
 * | Data Offset  | Key Size | Value Size |   | Data Offset  | Key Size | Value Size |
 * | (4 bytes)    | (2 bytes)| (2 bytes)  |   | (4 bytes)    | (2 bytes)| (2 bytes)  |
 * +--------------+----------+------------+   +--------------+----------+------------+
 * | 0x00000FD2   | 0x0005   | 0x0019     |   | 0x00000FB4   | 0x0005   | 0x001A     |
 * +--------------+----------+------------+   +--------------+----------+------------+
 *   Offset 4050     5 bytes    25 bytes        Offset 4020     5 bytes    26 bytes
 *        |                                          |
 *        |                                          |
 *        v                                          v
 * DataArea (grows from the end):
 *
 *          Slot 1 Data                           Slot 0 Data
 * +----------------------------+       +----------------------------+
 * | Key      | Value           |       | Key      | Value           |
 * | "user2"  | {"name":"Alice",|       | "user1"  | {"name":"John", |
 * |          |  "age":25}      |       |          |  "age":30}      |
 * +----------------------------+       +----------------------------+
 *   4020       4025                      4050       4055
 */
public class Page {
    private static final Logger LOGGER = Logger.getLogger(Page.class.getName());

    public static final int PAGE_SIZE = 4096; // 4KB per page - standard size

    private static final int SLOT_SIZE = 8; // Size of each slot entry
    private static final int SLOT_OFFSET_POS = 0;    // 4 bytes - position of data
    private static final int SLOT_KEY_SIZE_POS = 4;  // 2 bytes - size of key
    private static final int SLOT_VALUE_SIZE_POS = 6; // 2 bytes - size of value
    private static final int SLOT_REGION_START_OFFSET = PageHeader.headerSize();

    public int slotRegionEndOffset() {
        return SLOT_REGION_START_OFFSET + header.count() * SLOT_SIZE;
    }
    ByteBuffer buffer;
    private int dataStart;

    final PageHeader header;
    public Page(int pageId) {
        this.buffer = ByteBuffer.allocate(PAGE_SIZE);
        header = PageHeader.forLeafPage(buffer, pageId);
        this.dataStart = PAGE_SIZE; // Data area starts at the end of the page
        
        System.out.println("Created new Page with ID: " + pageId);
        System.out.println("  Page size: " + PAGE_SIZE + " bytes");
        System.out.println("  Header size: " + PageHeader.headerSize() + " bytes");
        System.out.println("  Initial free space: " + freeSpace() + " bytes");
    }

    public long getPageId() {
        return header.pageId();
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
    public static void ensureFileSize(FileChannel fileChannel, long numPages) throws IOException {
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
    private static long calculateRequiredSize(long numPages) {
        return getSize(numPages);
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
     * This method does not change the FileChannel's position.
     *
     * @param fileChannel The file channel to write to
     * @throws IOException If an I/O error occurs
     */
    public void writeTo(FileChannel fileChannel) throws IOException {
        // Calculate the correct position based on pageId
        long position = pageStartPosition();

        // Create a duplicate buffer so we don't affect the original
        ByteBuffer duplicate = buffer.duplicate();
        duplicate.position(0);
        duplicate.limit(PAGE_SIZE);

        // Use positional write which doesn't change the channel's position
        int totalWritten = 0;
        while (duplicate.hasRemaining()) {
            int written = fileChannel.write(duplicate, position + totalWritten);
            if (written <= 0) {
                throw new IOException("Failed to write page: no bytes written");
            }
            totalWritten += written;
        }

        // Verify we wrote the entire page
        if (totalWritten != PAGE_SIZE) {
            throw new IOException("Failed to write entire page: wrote " + totalWritten +
                    " bytes out of " + PAGE_SIZE);
        }
    }

    long pageStartPosition() {
        return header.pageId() * PAGE_SIZE;
    }

    void writeInt(int value) {
        buffer.putInt(value);
        buffer.rewind();
    }

    public int freeSpace() {
        return dataStart - slotRegionEndOffset();
    }

    public boolean put(byte[] keyBytes, byte[] valueBytes) {
        // Calculate required space
        int requiredSpace = keyBytes.length + valueBytes.length + SLOT_SIZE;
        
        System.out.println("Putting key-value pair - Key: " + new String(keyBytes) + 
                " (" + keyBytes.length + " bytes), Value: " + valueBytes.length + " bytes");
        System.out.println("  Required space: " + requiredSpace + " bytes");
        System.out.println("  Available free space: " + freeSpace() + " bytes");
        System.out.println("  Current data start: " + dataStart);
        System.out.println("  Current slot end: " + slotRegionEndOffset());

        // Check if there's enough free space
        if (requiredSpace > freeSpace()) {
            System.out.println("  Not enough space, returning false");
            return false; // Not enough space
        }

        // Calculate where to store data
        dataStart -= keyBytes.length + valueBytes.length;
        
        System.out.println("  Storing data at offset: " + dataStart);
        System.out.println("  Slot offset: " + (SLOT_REGION_START_OFFSET + header.count() * SLOT_SIZE));

        // Store slot information
        buffer.putInt(SLOT_REGION_START_OFFSET + header.count() * SLOT_SIZE, dataStart);
        buffer.putShort(SLOT_REGION_START_OFFSET + header.count() * SLOT_SIZE + SLOT_KEY_SIZE_POS, (short) keyBytes.length);
        buffer.putShort(SLOT_REGION_START_OFFSET + header.count() * SLOT_SIZE + SLOT_VALUE_SIZE_POS, (short) valueBytes.length);

        // Store actual data
        buffer.position(dataStart);
        buffer.put(keyBytes);
        buffer.put(valueBytes);

        // Increment count
        short existingCount = header.count();
        header.count((short) (existingCount + 1));
        
        System.out.println("  Insert complete, new count: " + header.count());
        System.out.println("  New free space: " + freeSpace() + " bytes");
        return true;
    }

    public byte[] get(byte[] key) {
        // Validate input
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }

        System.out.println("Searching for key with length: " + key.length);
        System.out.println("Page has " + header.count() + " entries");
        System.out.println("Page free space: " + freeSpace());

        // Linear search through all slots
        for (int i = 0; i < header.count(); i++) {
            int slotOffset = PageHeader.headerSize() + (i * SLOT_SIZE);
            
            System.out.println("Slot " + i + " offset: " + slotOffset);
            
            int dataOffset = buffer.getInt(slotOffset + SLOT_OFFSET_POS);
            short keySize = buffer.getShort(slotOffset + SLOT_KEY_SIZE_POS);
            short valueSize = buffer.getShort(slotOffset + SLOT_VALUE_SIZE_POS);
            
            System.out.println("  Data offset: " + dataOffset + ", key size: " + keySize + ", value size: " + valueSize);

            // Read the key
            byte[] currentKey = new byte[keySize];
            try {
                buffer.get(dataOffset, currentKey);
            } catch (Exception e) {
                System.out.println("  Error reading key: " + e.getMessage());
                e.printStackTrace();
                continue;
            }

            // Compare with our target key
            System.out.println("currentKey = " + new String(currentKey));
            System.out.println("key = " + new String(key));
            if (compareKeys(key, currentKey) == 0) {
                // Found the key, return the value
                byte[] value = new byte[valueSize];
                try {
                    buffer.get(dataOffset + keySize, value);
                    return value;
                } catch (Exception e) {
                    System.out.println("  Error reading value: " + e.getMessage());
                    e.printStackTrace();
                    return null;
                }
            }
        }

        // Key not found
        return null;
    }

    private int compareKeys(byte[] a, byte[] b) {
        // Handle null values
        if (a == null && b == null) {
            return 0;
        }
        if (a == null) {
            return -1;
        }
        if (b == null) {
            return 1;
        }

        // Use built-in comparison
        return Arrays.compare(a, b);
    }

    public short count() {
        return header.count();
    }

    /**
     * Reads the page data from a file channel at the correct offset based on pageId.
     * This method does not change the FileChannel's position.
     *
     * @param fileChannel The file channel to read from
     * @throws IOException If an I/O error occurs
     */
    public void readFrom(FileChannel fileChannel) throws IOException {
        long position = getPageId() * PAGE_SIZE;
        buffer.clear();
        int totalRead = 0;
        while (totalRead < PAGE_SIZE) {
            int read = fileChannel.read(buffer, position + totalRead);
            if (read < 0) {
                throw new IOException("Failed to read entire page: EOF reached");
            }
            totalRead += read;
        }
        buffer.flip();
    }

    /**
     * Static helper to ensure a page exists in the file and return its offset.
     * Throws IOException if the page does not exist.
     */
    public static long ensurePageExists(FileChannel channel, int pageId) throws IOException {
        long position = (long) pageId * PAGE_SIZE;
        if (position >= channel.size()) {
            throw new IOException("Page " + pageId + " does not exist");
        }
        return position;
    }
}
