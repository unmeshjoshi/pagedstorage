package com.bolddb;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents a row in a database table.
 * A row contains a primary key and a collection of attributes.
 */
public class Row {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    
    private final String primaryKey;
    private final Map<String, Object> attributes;

    public Row(String primaryKey) {
        this.primaryKey = primaryKey;
        this.attributes = new HashMap<>();
    }

    @JsonCreator
    public Row(
            @JsonProperty("primaryKey") String primaryKey, 
            @JsonProperty("attributes") Map<String, Object> attributes) {
        this.primaryKey = primaryKey;
        this.attributes = new HashMap<>(attributes);
    }

    public String getPrimaryKey() {
        return primaryKey;
    }

    public void setAttribute(String name, Object value) {
        attributes.put(name, value);
    }

    public Object getAttribute(String name) {
        return attributes.get(name);
    }

    public Map<String, Object> getAttributes() {
        return new HashMap<>(attributes);
    }
    
    /**
     * Serializes this row to a byte array using Jackson.
     *
     * @return The serialized row as a byte array
     */
    public byte[] serialize() {
        try {
            return OBJECT_MAPPER.writeValueAsBytes(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Error serializing row: " + e.getMessage(), e);
        }
    }
    
    /**
     * Deserializes a byte array into a Row object using Jackson.
     *
     * @param bytes The serialized row
     * @return The deserialized Row object
     */
    public static Row deserialize(byte[] bytes) {
        try {
            return OBJECT_MAPPER.readValue(bytes, Row.class);
        } catch (IOException e) {
            throw new RuntimeException("Error deserializing row: " + e.getMessage(), e);
        }
    }
    
    @Override
    public String toString() {
        return "Row{primaryKey='" + primaryKey + "', attributes=" + attributes + "}";
    }
} 