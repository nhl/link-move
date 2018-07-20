package com.nhl.link.move.runtime.task;

import com.nhl.link.move.CreateBuilder;
import com.nhl.link.move.CreateOrUpdateBuilder;
import com.nhl.link.move.DeleteBuilder;
import com.nhl.link.move.LmRuntimeException;
import com.nhl.link.move.SourceKeysBuilder;
import com.nhl.link.move.runtime.cayenne.ITargetCayenneService;
import com.nhl.link.move.runtime.extractor.IExtractorService;
import com.nhl.link.move.runtime.key.IKeyAdapterFactory;
import com.nhl.link.move.runtime.targetmodel.TargetEntity;
import com.nhl.link.move.runtime.targetmodel.TargetEntityMap;
import com.nhl.link.move.runtime.task.create.CreateTargetMapper;
import com.nhl.link.move.runtime.task.create.CreateTargetMerger;
import com.nhl.link.move.runtime.task.create.DefaultCreateBuilder;
import com.nhl.link.move.runtime.task.createorupdate.DefaultCreateOrUpdateBuilder;
import com.nhl.link.move.runtime.task.createorupdate.RowConverter;
import com.nhl.link.move.runtime.task.createorupdate.TargetMerger;
import com.nhl.link.move.runtime.task.delete.DefaultDeleteBuilder;
import com.nhl.link.move.runtime.task.sourcekeys.DefaultSourceKeysBuilder;
import com.nhl.link.move.runtime.token.ITokenManager;
import com.nhl.link.move.valueconverter.ValueConverterFactory;
import com.nhl.link.move.writer.ITargetPropertyWriterService;
import org.apache.cayenne.DataObject;
import org.apache.cayenne.di.Inject;
import org.apache.cayenne.map.ObjEntity;

public class TaskService implements ITaskService {

    private IExtractorService extractorService;
    private ITargetCayenneService targetCayenneService;
    private ITokenManager tokenManager;
    private IKeyAdapterFactory keyAdapterFactory;
    private TargetEntityMap targetEntityMap;
    private ITargetPropertyWriterService writerService;
    private ValueConverterFactory valueConverterFactory;

    public TaskService(
            @Inject IExtractorService extractorService,
            @Inject ITargetCayenneService targetCayenneService,
            @Inject ITokenManager tokenManager,
            @Inject IKeyAdapterFactory keyAdapterFactory,
            @Inject TargetEntityMap targetEntityMap,
            @Inject ITargetPropertyWriterService writerService,
            @Inject ValueConverterFactory valueConverterFactory) {

        this.extractorService = extractorService;
        this.targetCayenneService = targetCayenneService;
        this.tokenManager = tokenManager;
        this.keyAdapterFactory = keyAdapterFactory;
        this.targetEntityMap = targetEntityMap;
        this.writerService = writerService;
        this.valueConverterFactory = valueConverterFactory;
    }

    @Override
    public <T extends DataObject> CreateBuilder<T> create(Class<T> type) {

        CreateTargetMapper<T> mapper = new CreateTargetMapper<>(type);
        CreateTargetMerger<T> merger = new CreateTargetMerger<>(writerService.getWriterFactory(type));
        ObjEntity entity = lookupEntity(type);
        TargetEntity targetEntity = targetEntityMap.get(entity);
        RowConverter rowConverter = new RowConverter(targetEntity, valueConverterFactory);

        return new DefaultCreateBuilder(
                mapper,
                merger,
                rowConverter,
                targetCayenneService,
                extractorService,
                tokenManager);
    }

    @Override
    public <T extends DataObject> CreateOrUpdateBuilder<T> createOrUpdate(Class<T> type) {

        ObjEntity entity = lookupEntity(type);
        TargetEntity targetEntity = targetEntityMap.get(entity);
        MapperBuilder mapperBuilder = new MapperBuilder(entity, targetEntity, keyAdapterFactory);
        RowConverter rowConverter = new RowConverter(targetEntity, valueConverterFactory);
        TargetMerger<T> merger = new TargetMerger<>(targetEntity, writerService.getWriterFactory(type));

        return new DefaultCreateOrUpdateBuilder<>(
                type,
                merger,
                rowConverter,
                targetCayenneService,
                extractorService,
                tokenManager,
                mapperBuilder);
    }

    protected <T extends DataObject> ObjEntity lookupEntity(Class<T> type) {
        ObjEntity entity = targetCayenneService.entityResolver().getObjEntity(type);
        if (entity == null) {
            throw new LmRuntimeException("Java class " + type.getName() + " is not mapped in Cayenne");
        }
        return entity;
    }

    @Override
    public <T extends DataObject> SourceKeysBuilder extractSourceKeys(Class<T> type) {
        ObjEntity targetEntity = targetCayenneService.entityResolver().getObjEntity(type);
        return new DefaultSourceKeysBuilder(
                targetEntityMap.get(targetEntity),
                extractorService,
                tokenManager,
                keyAdapterFactory,
                valueConverterFactory);
    }

    @Override
    public SourceKeysBuilder extractSourceKeys(String targetEntityName) {
        ObjEntity targetEntity = targetCayenneService.entityResolver().getObjEntity(targetEntityName);
        return new DefaultSourceKeysBuilder(
                targetEntityMap.get(targetEntity),
                extractorService,
                tokenManager,
                keyAdapterFactory,
                valueConverterFactory);
    }

    @Override
    public <T extends DataObject> DeleteBuilder<T> delete(Class<T> type) {

        ObjEntity entity = targetCayenneService.entityResolver().getObjEntity(type);
        if (entity == null) {
            throw new LmRuntimeException("Java class " + type.getName() + " is not mapped in Cayenne");
        }

        TargetEntity targetEntity = targetEntityMap.get(entity);
        MapperBuilder mapperBuilder = new MapperBuilder(entity, targetEntity, keyAdapterFactory);

        return new DefaultDeleteBuilder<>(
                type,
                targetCayenneService,
                tokenManager,
                this,
                mapperBuilder);
    }

}
