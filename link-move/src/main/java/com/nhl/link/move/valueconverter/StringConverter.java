package com.nhl.link.move.valueconverter;

public class StringConverter extends SingleTypeConverter<String> {

    @Override
    protected Class<String> targetType() {
        return String.class;
    }

    @Override
    protected String convertNotNull(Object value, int scale) {
        return value.toString();
    }
}
