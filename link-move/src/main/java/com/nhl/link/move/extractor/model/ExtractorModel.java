package com.nhl.link.move.extractor.model;

import com.nhl.link.move.RowAttribute;
import com.nhl.link.move.extractor.Extractor;

import java.util.Collection;
import java.util.Map;

/**
 * A model of a single {@link Extractor}.
 *
 * @since 1.4
 */
public interface ExtractorModel {

    String DEFAULT_NAME = "default_extractor";

    String getName();

    String getType();

    /**
     * @return Collection of connector IDs
     * @since 2.2
     */
    Collection<String> getConnectorIds();

    /**
     * @deprecated since 2.9 as we switched to multi-value properties. Use {@link #getPropertyValue(String)} or
     * {@link #getPropertyValues(String)} instead.
     */
    @Deprecated
    Map<String, String> getProperties();

    /**
     * @since 2.9
     */
    Collection<String> getPropertyValues(String propertyName);

    /**
     * @since 2.9
     */
    String getPropertyValue(String propertyName);

    RowAttribute[] getAttributes();

    /**
     * Returns load timestamp for this model.
     */
    long getLoadedOn();
}
