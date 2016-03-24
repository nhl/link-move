package com.nhl.link.move;

public interface RowAttribute {
	int getOrdinal();

	Class<?> type();

	String getSourceName();

	String getTargetPath();
}
