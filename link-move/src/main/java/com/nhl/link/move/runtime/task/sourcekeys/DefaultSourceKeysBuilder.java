package com.nhl.link.move.runtime.task.sourcekeys;

import com.nhl.link.move.LmTask;
import com.nhl.link.move.SourceKeysBuilder;
import com.nhl.link.move.annotation.AfterSourceKeysCollected;
import com.nhl.link.move.annotation.AfterSourceRowsExtracted;
import com.nhl.link.move.extractor.model.ExtractorName;
import com.nhl.link.move.log.LmLogger;
import com.nhl.link.move.mapper.Mapper;
import com.nhl.link.move.runtime.extractor.IExtractorService;
import com.nhl.link.move.runtime.key.IKeyAdapterFactory;
import com.nhl.link.move.runtime.targetmodel.TargetEntity;
import com.nhl.link.move.runtime.task.BaseTaskBuilder;
import com.nhl.link.move.runtime.task.common.StatsIncrementor;
import com.nhl.link.move.runtime.task.createorupdate.RowConverter;
import com.nhl.link.move.valueconverter.ValueConverterFactory;

import java.lang.annotation.Annotation;

/**
 * @since 1.3
 */
public class DefaultSourceKeysBuilder extends BaseTaskBuilder<DefaultSourceKeysBuilder, SourceKeysSegment, SourceKeysStage> implements SourceKeysBuilder {

    private final IExtractorService extractorService;
    private final SourceMapperBuilder mapperBuilder;

    private final TargetEntity targetEntity;
    private final ValueConverterFactory valueConverterFactory;

    private ExtractorName sourceExtractorName;

    public DefaultSourceKeysBuilder(
            TargetEntity targetEntity,
            IExtractorService extractorService,
            IKeyAdapterFactory keyAdapterFactory,
            ValueConverterFactory valueConverterFactory,
            LmLogger logger) {

        super(logger);

        this.extractorService = extractorService;
        this.mapperBuilder = new SourceMapperBuilder(targetEntity, keyAdapterFactory);
        this.targetEntity = targetEntity;
        this.valueConverterFactory = valueConverterFactory;

        setupStatsCallbacks();
    }

    protected void setupStatsCallbacks() {
        StatsIncrementor incrementor = StatsIncrementor.instance();
        stage(SourceKeysStage.EXTRACT_SOURCE_ROWS, incrementor::sourceRowsExtracted);
        stage(SourceKeysStage.COLLECT_SOURCE_KEYS, incrementor::sourceKeysCollected);
    }

    @Override
    protected Class<? extends Annotation>[] supportedListenerAnnotations() {
        return new Class[]{AfterSourceRowsExtracted.class, AfterSourceKeysCollected.class};
    }

    @Override
    public LmTask task() throws IllegalStateException {

        if (sourceExtractorName == null) {
            throw new IllegalStateException("Required 'extractorName' is not set");
        }

        return new SourceKeysTask(sourceExtractorName, batchSize, extractorService, createProcessor(), logger);
    }

    private SourceKeysSegmentProcessor createProcessor() {
        Mapper mapper = mapperBuilder.build();
        SourceKeysCollector sourceMapper = new SourceKeysCollector(mapper);
        RowConverter converter = new RowConverter(targetEntity, valueConverterFactory);
        return new SourceKeysSegmentProcessor(converter, sourceMapper, getCallbackExecutor());
    }

    @Override
    public SourceKeysBuilder sourceExtractor(ExtractorName extractorName) {
        sourceExtractorName = extractorName;
        return this;
    }

    @Override
    public SourceKeysBuilder matchBy(Mapper mapper) {
        mapperBuilder.matchBy(mapper);
        return this;
    }

    @Override
    public SourceKeysBuilder matchBy(String... columns) {
        mapperBuilder.matchBy(columns);
        return this;
    }
}
