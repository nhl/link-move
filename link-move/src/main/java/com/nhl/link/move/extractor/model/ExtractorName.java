package com.nhl.link.move.extractor.model;

import org.apache.cayenne.util.HashCodeBuilder;

/**
 * Encapsulates a fully-qualified name of an {@link ExtractorModel}.
 *
 * @since 1.4
 */
public class ExtractorName {

    /**
     * @since 3.0.0
     */
    public static final String DEFAULT_NAME = "default_extractor";

    private final String location;
    private final String name;

    public static ExtractorName create(String location, String name) {

        if (location == null) {
            throw new NullPointerException("Null location");
        }

        if (name == null) {
            throw new NullPointerException("Null name");
        }

        return new ExtractorName(location, name);
    }

    private ExtractorName(String location, String name) {
        this.location = location;
        this.name = name;
    }

    public String getLocation() {
        return location;
    }

    public String getName() {
        return name;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }

        if (!(object instanceof ExtractorName)) {
            return false;
        }

        ExtractorName extractorName = (ExtractorName) object;

        if (!name.equals(extractorName.name)) {
            return false;
        }

        if (!location.equals(extractorName.location)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        HashCodeBuilder builder = new HashCodeBuilder(13, 19);
        return builder.append(location.hashCode()).append(name.hashCode()).toHashCode();
    }

    @Override
    public String toString() {
        return DEFAULT_NAME.equals(name) ? location : location + "#" + name;
    }
}
