package com.nhl.link.move.runtime.targetmodel;

/**
 * A target attribute.
 *
 * @since 2.6
 */
public class TargetAttribute {

    private String normalizedPath;
    private int scale;
    private String type;

    public TargetAttribute(String normalizedPath, int scale, String type) {
        this.normalizedPath = normalizedPath;
        this.scale = scale;
        this.type = type;
    }

    public String getType() {
        return type;
    }

    public int getScale() {
        return scale;
    }

    public String getNormalizedPath() {
        return normalizedPath;
    }
}
