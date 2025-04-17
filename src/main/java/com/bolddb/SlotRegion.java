package com.bolddb;

import java.nio.ByteBuffer;

public class SlotRegion {
    private final ByteBuffer buffer;
    private final int regionStart;
    private final int slotSize;
    private final PageHeader header;

    public SlotRegion(ByteBuffer buffer, int regionStart, int slotSize, PageHeader header) {
        this.buffer = buffer;
        this.regionStart = regionStart;
        this.slotSize = slotSize;
        this.header = header;
    }

    public int getSlotCount() {
        return header.count();
    }

    public void incrementCount() {
        header.count((short) (header.count() + 1));
    }

    public void writeSlot(int slotIndex, int offset, int keySize, int valueSize) {
        int base = regionStart + slotIndex * slotSize;
        buffer.putInt(base, offset);
        buffer.putShort(base + 4, (short) keySize);
        buffer.putShort(base + 6, (short) valueSize);
    }

    public int getOffset(int slotIndex) {
        return buffer.getInt(regionStart + slotIndex * slotSize);
    }
    public int getKeySize(int slotIndex) {
        return buffer.getShort(regionStart + slotIndex * slotSize + 4) & 0xFFFF;
    }
    public int getValueSize(int slotIndex) {
        return buffer.getShort(regionStart + slotIndex * slotSize + 6) & 0xFFFF;
    }

    public int findSlotForKey(byte[] key, DataRegion dataRegion) {
        for (int i = 0; i < header.count(); i++) {
            int offset = getOffset(i);
            int keySize = getKeySize(i);
            if (dataRegion.keyEquals(offset, key, keySize)) {
                return i;
            }
        }
        return -1;
    }
}
