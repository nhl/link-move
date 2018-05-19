package com.nhl.link.move.runtime.targetmodel;

import java.util.Optional;

/**
 * A target attribute.
 *
 * @since 2.6
 */
public class TargetAttribute {

    private String normalizedPath;
    private int scale;
    private String javaType;
    private Optional<ForeignKey> foreignKey;

    public TargetAttribute(String normalizedPath, int scale, String javaType, Optional<ForeignKey> foreignKey) {
        this.normalizedPath = normalizedPath;
        this.scale = scale;
        this.javaType = javaType;
        this.foreignKey = foreignKey;
    }

    public String getJavaType() {
        return javaType;
    }

    public int getScale() {
        return scale;
    }

    public String getNormalizedPath() {
        return normalizedPath;
    }

    public Optional<ForeignKey> getForeignKey() {
        return foreignKey;
    }
}
