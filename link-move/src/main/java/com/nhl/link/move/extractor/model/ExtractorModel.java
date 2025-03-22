package com.nhl.link.move.extractor.model;

import com.nhl.link.move.RowAttribute;
import com.nhl.link.move.extractor.Extractor;

import java.util.Collection;

/**
 * A model of a single {@link Extractor}.
 *
 * @since 1.4
 */
public interface ExtractorModel {

    String getName();

    String getType();

    /**
     * @return Collection of connector IDs
     * @since 2.2
     */
    Collection<String> getConnectorIds();

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
