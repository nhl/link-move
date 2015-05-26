package com.nhl.link.etl;

/**
 * Describes a single entry in a data row.
 */
public class RowAttribute {

	private Class<?> type;
	private String sourceName;
	private String targetName;
	private int ordinal;

	public RowAttribute(Class<?> type, String sourceName, String targetName, int ordinal) {
		this.type = type;
		this.sourceName = sourceName;
		this.targetName = targetName;
		this.ordinal = ordinal;
	}

	public int ordinal() {
		return ordinal;
	}

	public Class<?> type() {
		return type;
	}

	public String sourceName() {
		return sourceName;
	}

	public String targetName() {
		return targetName;
	}

	@Override
	public String toString() {
		StringBuilder buffer = new StringBuilder();
		buffer.append("{sourceName:").append(sourceName).append(",targetName:").append(targetName).append(",ordinal:")
				.append(ordinal).append("}");
		return buffer.toString();
	}
}
