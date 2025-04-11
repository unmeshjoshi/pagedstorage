package com.bolddb;

import java.nio.ByteBuffer;

public class PageHeader {
    private static final int HEADER_SIZE = 16;

    // Field offsets
    private static final int PAGE_ID_OFFSET = 0;    // 8 bytes
    private static final int FLAGS_OFFSET = 8;      // 2 bytes
    private static final int COUNT_OFFSET = 10;     // 2 bytes
    private static final int OVERFLOW_OFFSET = 12;  // 4 bytes


    public enum PageType {
        BRANCH(0x01),
        LEAF(0x02),
        META(0x04),
        FREELIST(0x10);

        private final short flag;

        PageType(int flag) {
            this.flag = (short)flag;
        }

        public short flag() { return flag; }

        public static PageType fromFlags(short flags) {
            for (PageType type : values()) {
                if ((flags & type.flag) != 0) {
                    return type;
                }
            }
            throw new IllegalArgumentException("Invalid page flags: " + flags);
        }
    }

    private final ByteBuffer buffer;

    private PageHeader(ByteBuffer pageBuffer) {
        this.buffer = pageBuffer;
    }

    private static PageHeader create(ByteBuffer buffer, long pageId, PageType type) {
        var header = new PageHeader(buffer);
        header.buffer.putLong(PAGE_ID_OFFSET, pageId);
        header.buffer.putShort(FLAGS_OFFSET, type.flag());
        header.buffer.putShort(COUNT_OFFSET, (short)0);
        header.buffer.putInt(OVERFLOW_OFFSET, 0);
        return header;
    }

    public static PageHeader forBranchPage(ByteBuffer buffer, long pageId) {
        return create(buffer, pageId, PageType.BRANCH);
    }

    public static PageHeader forLeafPage(ByteBuffer buffer, long pageId) {
        return create(buffer, pageId, PageType.LEAF);
    }

    public static PageHeader forMetaPage(ByteBuffer buffer, long pageId) {
        return create(buffer, pageId, PageType.META);
    }

    public static PageHeader forFreelistPage(ByteBuffer buffer, long pageId) {
        return create(buffer, pageId, PageType.FREELIST);
    }

    // Only mutable field
    public void count(short count) {
        buffer.putShort(COUNT_OFFSET, count);
    }

    // Getters
    public long pageId() { return buffer.getLong(PAGE_ID_OFFSET); }
    public short flags() { return buffer.getShort(FLAGS_OFFSET); }
    public short count() { return buffer.getShort(COUNT_OFFSET); }
    public int overflow() { return buffer.getInt(OVERFLOW_OFFSET); }

    // Helper methods using enum
    public PageType type() { return PageType.fromFlags(flags()); }
    public boolean isBranchPage() { return type() == PageType.BRANCH; }
    public boolean isLeafPage() { return type() == PageType.LEAF; }
    public boolean isMetaPage() { return type() == PageType.META; }
    public boolean isFreelistPage() { return type() == PageType.FREELIST; }

    public static int headerSize() { return HEADER_SIZE; }
}