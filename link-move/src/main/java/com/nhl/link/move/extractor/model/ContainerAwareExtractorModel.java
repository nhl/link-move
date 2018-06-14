package com.nhl.link.move.extractor.model;

import com.nhl.link.move.RowAttribute;

import java.util.Collection;
import java.util.Map;

/**
 * An {@link ExtractorModel} decorator that allows a model to inherit some
 * properties from the parent {@link ExtractorModelContainer}.
 * 
 * @since 1.4
 */
public class ContainerAwareExtractorModel implements ExtractorModel {

	private ExtractorModelContainer parent;
	private ExtractorModel delegate;

	public ContainerAwareExtractorModel(ExtractorModelContainer parent, ExtractorModel delegate) {
		this.delegate = delegate;
		this.parent = parent;
	}

	@Override
	public String getName() {
		return delegate.getName();
	}

	@Override
	public long getLoadedOn() {
		long loadedOn = delegate.getLoadedOn();
		return loadedOn > 0 ? loadedOn : parent.getLoadedOn();
	}

	@Override
	public String getType() {
		String type = delegate.getType();
		return type != null ? type : parent.getType();
	}

	@Override
	public Collection<String> getConnectorIds() {
		return delegate.getConnectorIds().isEmpty() ? parent.getConnectorIds() : delegate.getConnectorIds();
	}

	@Override
	public Map<String, Collection<String>> getProperties() {
		return delegate.getProperties();
	}

	@Override
	public String getSingletonProperty(String propertyName) {
		return delegate.getSingletonProperty(propertyName);
	}

	@Override
	public RowAttribute[] getAttributes() {
		return delegate.getAttributes();
	}

}
