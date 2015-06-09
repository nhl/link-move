package com.nhl.link.etl;

/**
 * A descriptor of a {@link Row} attribute. May contain optional path expression
 * that maps the attribute to a target attribute.
 */
public class RowAttribute {

	private Class<?> type;
	private String sourceName;
	private String targetPath;
	private int ordinal;

	public RowAttribute(Class<?> type, String sourceName, String targetName, int ordinal) {
		this.type = type;
		this.sourceName = sourceName;
		this.targetPath = targetName;
		this.ordinal = ordinal;
	}

	/**
	 * Returns a position of the attribute in a row.
	 */
	public int getOrdinal() {
		return ordinal;
	}

	public Class<?> type() {
		return type;
	}

	/**
	 * Returns a String name of the attribute as provided by the source.
	 */
	public String getSourceName() {
		return sourceName;
	}

	/**
	 * Returns a path expression that maps source Row value to a target
	 * property.
	 */
	public String getTargetPath() {
		return targetPath;
	}

	@Override
	public String toString() {
		StringBuilder buffer = new StringBuilder();
		buffer.append("{sourceName:").append(sourceName).append(",targetName:").append(targetPath).append(",ordinal:")
				.append(ordinal).append("}");
		return buffer.toString();
	}
}
