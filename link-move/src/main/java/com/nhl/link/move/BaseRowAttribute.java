package com.nhl.link.move;

/**
 * A descriptor of a source row attribute. May contain optional path expression that maps the attribute to a target
 * attribute.
 */
public class BaseRowAttribute implements RowAttribute {

    private final Class<?> type;
    private final String sourceName;
    private final String targetPath;
    private final int ordinal;

    public BaseRowAttribute(Class<?> type, String sourceName, String targetName, int ordinal) {
        this.type = type;
        this.sourceName = sourceName;
        this.targetPath = targetName;
        this.ordinal = ordinal;
    }

    /**
     * Returns a position of the attribute in a row.
     */
    @Override
    public int getOrdinal() {
        return ordinal;
    }

    @Override
    public Class<?> type() {
        return type;
    }

    /**
     * Returns a String name of the attribute as provided by the source.
     */
    @Override
    public String getSourceName() {
        return sourceName;
    }

    /**
     * Returns a path expression that maps source Row value to a target
     * property.
     */
    @Override
    public String getTargetPath() {
        return targetPath;
    }

    @Override
    public String toString() {
        return "{sourceName:" + sourceName + ",targetName:" + targetPath + ",ordinal:" + ordinal + "}";
    }
}
