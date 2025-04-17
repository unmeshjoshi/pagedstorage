package com.bolddb;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class DataRegion {
    private final ByteBuffer buffer;
    private int dataStart;

    public DataRegion(ByteBuffer buffer, int pageSize) {
        this.buffer = buffer;
        this.dataStart = pageSize;
    }

    public int getDataStart() {
        return dataStart;
    }

    public int allocateRecord(int recordSize) {
        dataStart -= recordSize;
        return dataStart;
    }

    public void writeRecord(int offset, byte[] key, byte[] value) {
        for (int i = 0; i < key.length; i++) buffer.put(offset + i, key[i]);
        for (int i = 0; i < value.length; i++) buffer.put(offset + key.length + i, value[i]);
    }

    public boolean keyEquals(int offset, byte[] key, int keySize) {
        for (int i = 0; i < keySize; i++) {
            if (buffer.get(offset + i) != key[i]) return false;
        }
        return true;
    }

    public byte[] readValue(int offset, int keySize, int valueSize) {
        byte[] value = new byte[valueSize];
        for (int i = 0; i < valueSize; i++) {
            value[i] = buffer.get(offset + keySize + i);
        }
        return value;
    }
}
