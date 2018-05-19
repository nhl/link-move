package com.nhl.link.move.runtime.targetmodel;

/**
 * A target attribute.
 *
 * @since 2.6
 */
public class TargetAttribute {

    private String normalizedPath;
    private int scale;
    private String javaType;

    public TargetAttribute(String normalizedPath, int scale, String javaType) {
        this.normalizedPath = normalizedPath;
        this.scale = scale;
        this.javaType = javaType;
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
}
