package com.nhl.link.move.extractor.model;

import com.nhl.link.move.LmRuntimeException;
import com.nhl.link.move.RowAttribute;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @since 1.4
 */
public class MutableExtractorModel implements ExtractorModel {

    private final String name;
    private final Set<String> connectorIds;
    private final Map<String, Collection<String>> properties;

    private String type;
    private long loadedOn;
    private RowAttribute[] attributes;

    public MutableExtractorModel(String name) {
        this.name = name;
        this.connectorIds = new HashSet<>();
        this.properties = new HashMap<>();
    }

    @Override
    public Collection<String> getPropertyValues(String propertyName) {
        Collection<String> values = properties.get(propertyName);

        // returning an immutable collection. While this class has "Mutable" in its name, mutation should happen
        // via special methods, and "getPropertyValues" (defined in the ExtractorModel interface) should
        // not allow to change properties implicitly
        return values != null
                ? Collections.unmodifiableCollection(values)
                : Collections.emptyList();
    }

    @Override
    public String getPropertyValue(String propertyName) {
        Collection<String> values = properties.get(propertyName);
        if (values == null || values.isEmpty()) {
            return null;
        } else if (values.size() > 1) {
            throw new LmRuntimeException("Multiple values are present for property: " + propertyName);
        }
        return values.iterator().next();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @Override
    public Collection<String> getConnectorIds() {
        return connectorIds;
    }

    @Override
    public RowAttribute[] getAttributes() {
        return attributes;
    }

    public void setAttributes(RowAttribute... rowKeys) {
        this.attributes = rowKeys;
    }

    @Override
    public long getLoadedOn() {
        return loadedOn;
    }

    public void setLoadedOn(long loadedOn) {
        this.loadedOn = loadedOn;
    }

    /**
     * @since 2.2
     */
    public void addConnectorId(String connectorId) {
        this.connectorIds.add(connectorId);
    }

    /**
     * @since 2.9
     */
    public void addProperty(String name, String value) {
        this.properties.computeIfAbsent(name, it -> new HashSet<>()).add(value);
    }

    /**
     * @since 2.9
     */
    public void clearProperties() {
        properties.clear();
    }
}
