package com.nhl.link.move.runtime.task;

import com.nhl.link.move.CreateBuilder;
import com.nhl.link.move.CreateOrUpdateBuilder;
import com.nhl.link.move.DeleteBuilder;
import com.nhl.link.move.LmRuntimeException;
import com.nhl.link.move.DeleteAllBuilder;
import com.nhl.link.move.SourceKeysBuilder;
import com.nhl.link.move.log.LmLogger;
import com.nhl.link.move.runtime.cayenne.ITargetCayenneService;
import com.nhl.link.move.runtime.extractor.IExtractorService;
import com.nhl.link.move.runtime.key.IKeyAdapterFactory;
import com.nhl.link.move.runtime.targetmodel.TargetEntity;
import com.nhl.link.move.runtime.targetmodel.TargetEntityMap;
import com.nhl.link.move.runtime.task.common.FkResolver;
import com.nhl.link.move.runtime.task.create.CreateTargetMapper;
import com.nhl.link.move.runtime.task.create.CreateTargetMerger;
import com.nhl.link.move.runtime.task.create.DefaultCreateBuilder;
import com.nhl.link.move.runtime.task.createorupdate.CreateOrUpdateTargetMerger;
import com.nhl.link.move.runtime.task.createorupdate.DefaultCreateOrUpdateBuilder;
import com.nhl.link.move.runtime.task.createorupdate.RowConverter;
import com.nhl.link.move.runtime.task.delete.DefaultDeleteBuilder;
import com.nhl.link.move.runtime.task.deleteall.DefaultDeleteAllBuilder;
import com.nhl.link.move.runtime.task.sourcekeys.DefaultSourceKeysBuilder;
import com.nhl.link.move.runtime.token.ITokenManager;
import com.nhl.link.move.valueconverter.ValueConverterFactory;
import com.nhl.link.move.writer.ITargetPropertyWriterService;
import org.apache.cayenne.di.Inject;
import org.apache.cayenne.map.ObjEntity;

public class TaskService implements ITaskService {

    private final IExtractorService extractorService;
    private final ITargetCayenneService targetCayenneService;

    private final ITokenManager tokenManager;
    private final IKeyAdapterFactory keyAdapterFactory;
    private final TargetEntityMap targetEntityMap;
    private final ITargetPropertyWriterService writerService;
    private final ValueConverterFactory valueConverterFactory;
    private final LmLogger logger;

    public TaskService(
            @Inject IExtractorService extractorService,
            @Inject ITargetCayenneService targetCayenneService,
            @Inject ITokenManager tokenManager,
            @Inject IKeyAdapterFactory keyAdapterFactory,
            @Inject TargetEntityMap targetEntityMap,
            @Inject ITargetPropertyWriterService writerService,
            @Inject ValueConverterFactory valueConverterFactory,
            @Inject LmLogger logger) {

        this.extractorService = extractorService;
        this.targetCayenneService = targetCayenneService;
        this.tokenManager = tokenManager;
        this.keyAdapterFactory = keyAdapterFactory;
        this.targetEntityMap = targetEntityMap;
        this.writerService = writerService;
        this.valueConverterFactory = valueConverterFactory;
        this.logger = logger;
    }

    @Override
    public CreateBuilder create(Class<?> type) {

        ObjEntity entity = lookupEntity(type);
        TargetEntity targetEntity = targetEntityMap.get(entity);
        CreateTargetMerger merger = new CreateTargetMerger(writerService.getWriterFactory(type));
        FkResolver fkResolver = new FkResolver(targetEntity);
        RowConverter rowConverter = new RowConverter(targetEntity, valueConverterFactory);

        return new DefaultCreateBuilder(
                new CreateTargetMapper(type),
                merger,
                fkResolver,
                rowConverter,
                targetCayenneService,
                extractorService,
                tokenManager,
                logger);
    }

    @Override
    public CreateOrUpdateBuilder createOrUpdate(Class<?> type) {

        ObjEntity entity = lookupEntity(type);
        TargetEntity targetEntity = targetEntityMap.get(entity);
        MapperBuilder mapperBuilder = new MapperBuilder(entity, targetEntity, keyAdapterFactory);
        RowConverter rowConverter = new RowConverter(targetEntity, valueConverterFactory);
        CreateOrUpdateTargetMerger merger = new CreateOrUpdateTargetMerger(writerService.getWriterFactory(type));
        FkResolver fkResolver = new FkResolver(targetEntity);

        return new DefaultCreateOrUpdateBuilder(
                type,
                merger,
                fkResolver,
                rowConverter,
                targetCayenneService,
                extractorService,
                tokenManager,
                mapperBuilder,
                logger);
    }

    protected ObjEntity lookupEntity(Class<?> type) {
        ObjEntity entity = targetCayenneService.entityResolver().getObjEntity(type);
        if (entity == null) {
            throw new LmRuntimeException("Java class " + type.getName() + " is not mapped in Cayenne");
        }
        return entity;
    }

    @Override
    public SourceKeysBuilder extractSourceKeys(Class<?> type) {
        ObjEntity targetEntity = targetCayenneService.entityResolver().getObjEntity(type);
        return new DefaultSourceKeysBuilder(
                targetEntityMap.get(targetEntity),
                extractorService,
                tokenManager,
                keyAdapterFactory,
                valueConverterFactory,
                logger);
    }

    @Override
    public SourceKeysBuilder extractSourceKeys(String targetEntityName) {
        ObjEntity targetEntity = targetCayenneService.entityResolver().getObjEntity(targetEntityName);
        return new DefaultSourceKeysBuilder(
                targetEntityMap.get(targetEntity),
                extractorService,
                tokenManager,
                keyAdapterFactory,
                valueConverterFactory,
                logger);
    }

    @Override
    public DeleteBuilder delete(Class<?> type) {

        ObjEntity entity = targetCayenneService.entityResolver().getObjEntity(type);
        if (entity == null) {
            throw new LmRuntimeException("Java class " + type.getName() + " is not mapped in Cayenne");
        }

        TargetEntity targetEntity = targetEntityMap.get(entity);
        MapperBuilder mapperBuilder = new MapperBuilder(entity, targetEntity, keyAdapterFactory);

        return new DefaultDeleteBuilder(
                type,
                targetCayenneService,
                tokenManager,
                this,
                mapperBuilder,
                logger);
    }

    @Override
    public DeleteAllBuilder deleteAll(Class<?> type) {
        ObjEntity entity = lookupEntity(type);

        return new DefaultDeleteAllBuilder(
                type,
                targetCayenneService,
                tokenManager,
                entity.getDbEntity(),
                logger);
    }
}
