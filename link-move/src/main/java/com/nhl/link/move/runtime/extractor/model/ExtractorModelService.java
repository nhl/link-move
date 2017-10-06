package com.nhl.link.move.runtime.extractor.model;

import com.nhl.link.move.LmRuntimeException;
import com.nhl.link.move.extractor.model.ExtractorModel;
import com.nhl.link.move.extractor.model.ExtractorModelContainer;
import com.nhl.link.move.extractor.model.ExtractorName;
import com.nhl.link.move.extractor.parser.IExtractorModelParser;
import com.nhl.link.move.resource.ResourceResolver;
import org.apache.cayenne.di.Inject;

import java.io.IOException;
import java.io.Reader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ExtractorModelService implements IExtractorModelService {

    private IExtractorModelParser parser;
    private ResourceResolver modelLoader;
    private ConcurrentMap<String, ExtractorModelContainer> containers;

    public ExtractorModelService(
            @Inject ResourceResolver modelLoader,
            @Inject IExtractorModelParser parser) {

        this.parser = parser;
        this.modelLoader = modelLoader;
        this.containers = new ConcurrentHashMap<>();
    }

    @Override
    public ExtractorModel get(ExtractorName name) {

        ExtractorModelContainer c = getContainer(name.getLocation());

        ExtractorModel m = c.getExtractor(name.getName());
        if (m == null) {
            throw new LmRuntimeException("No extractor model found for name: " + name);
        }

        return m;
    }

    protected ExtractorModelContainer getContainer(String location) {

        ExtractorModelContainer c = containers.get(location);
        if (c == null || modelLoader.needsReload(c.getLocation(), c.getLoadedOn())) {
            c = loadContainer(location);

            // not worried about overriding a fresh container loaded by other
            // threads... In fact using 'putIfAbsent' instead of 'put' will
            // fail, as we may have an expired container stored in the map...

            containers.put(location, c);
        }

        return c;
    }

    protected ExtractorModelContainer loadContainer(String location) {
        try (Reader in = modelLoader.reader(location)) {
            return parser.parse(location, in);
        } catch (IOException e) {
            throw new LmRuntimeException("Error reading extractor config XML", e);
        }
    }
}
