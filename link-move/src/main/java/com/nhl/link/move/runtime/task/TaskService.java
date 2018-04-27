package com.nhl.link.move.runtime.task;

import com.nhl.link.move.CreateBuilder;
import com.nhl.link.move.CreateOrUpdateBuilder;
import com.nhl.link.move.DeleteBuilder;
import com.nhl.link.move.LmRuntimeException;
import com.nhl.link.move.SourceKeysBuilder;
import com.nhl.link.move.runtime.cayenne.ITargetCayenneService;
import com.nhl.link.move.runtime.extractor.IExtractorService;
import com.nhl.link.move.runtime.key.IKeyAdapterFactory;
import com.nhl.link.move.runtime.path.EntityPathNormalizer;
import com.nhl.link.move.runtime.path.IPathNormalizer;
import com.nhl.link.move.runtime.task.create.DefaultCreateBuilder;
import com.nhl.link.move.runtime.task.create.TargetCreator;
import com.nhl.link.move.runtime.task.createorupdate.DefaultCreateOrUpdateBuilder;
import com.nhl.link.move.runtime.task.createorupdate.RowConverter;
import com.nhl.link.move.runtime.task.delete.DefaultDeleteBuilder;
import com.nhl.link.move.runtime.task.sourcekeys.DefaultSourceKeysBuilder;
import com.nhl.link.move.runtime.token.ITokenManager;
import com.nhl.link.move.writer.ITargetPropertyWriterService;
import org.apache.cayenne.DataObject;
import org.apache.cayenne.di.Inject;
import org.apache.cayenne.map.ObjEntity;

public class TaskService implements ITaskService {

    private IExtractorService extractorService;
    private ITargetCayenneService targetCayenneService;
    private ITokenManager tokenManager;
    private IKeyAdapterFactory keyAdapterFactory;
    private IPathNormalizer pathNormalizer;
    private ITargetPropertyWriterService writerService;

    public TaskService(
            @Inject IExtractorService extractorService,
            @Inject ITargetCayenneService targetCayenneService,
            @Inject ITokenManager tokenManager,
            @Inject IKeyAdapterFactory keyAdapterFactory,
            @Inject IPathNormalizer pathNormalizer,
            @Inject ITargetPropertyWriterService writerService) {

        this.extractorService = extractorService;
        this.targetCayenneService = targetCayenneService;
        this.tokenManager = tokenManager;
        this.keyAdapterFactory = keyAdapterFactory;
        this.pathNormalizer = pathNormalizer;
        this.writerService = writerService;
    }

    @Override
    public <T extends DataObject> CreateBuilder<T> create(Class<T> type) {

        TargetCreator<T> creator = new TargetCreator<>(type, writerService.getWriterFactory(type));
        ObjEntity entity = lookupEntity(type);
        EntityPathNormalizer entityPathNormalizer = pathNormalizer.normalizer(entity);
        RowConverter rowConverter = new RowConverter(entityPathNormalizer);

        return new DefaultCreateBuilder(
                creator,
                rowConverter,
                targetCayenneService,
                extractorService,
                tokenManager);
    }

    @Override
    public <T extends DataObject> CreateOrUpdateBuilder<T> createOrUpdate(Class<T> type) {
        ObjEntity entity = lookupEntity(type);
        EntityPathNormalizer entityPathNormalizer = pathNormalizer.normalizer(entity);
        MapperBuilder mapperBuilder = new MapperBuilder(entity, entityPathNormalizer, keyAdapterFactory);
        RowConverter rowConverter = new RowConverter(entityPathNormalizer);

        return new DefaultCreateOrUpdateBuilder<>(
                type,
                rowConverter,
                targetCayenneService,
                extractorService,
                tokenManager,
                mapperBuilder,
                writerService);
    }

    protected <T extends DataObject> ObjEntity lookupEntity(Class<T> type) {
        ObjEntity entity = targetCayenneService.entityResolver().getObjEntity(type);
        if (entity == null) {
            throw new LmRuntimeException("Java class " + type.getName() + " is not mapped in Cayenne");
        }
        return entity;
    }

    protected EntityPathNormalizer entityPathNormalizer(ObjEntity entity) {
        return pathNormalizer.normalizer(entity);
    }

    @Override
    public <T extends DataObject> SourceKeysBuilder extractSourceKeys(Class<T> type) {
        ObjEntity targetEntity = targetCayenneService.entityResolver().getObjEntity(type);
        return new DefaultSourceKeysBuilder(pathNormalizer.normalizer(targetEntity), extractorService, tokenManager,
                keyAdapterFactory);
    }

    @Override
    public SourceKeysBuilder extractSourceKeys(String targetEntityName) {
        ObjEntity targetEntity = targetCayenneService.entityResolver().getObjEntity(targetEntityName);
        return new DefaultSourceKeysBuilder(pathNormalizer.normalizer(targetEntity), extractorService, tokenManager,
                keyAdapterFactory);
    }

    @Override
    public <T extends DataObject> DeleteBuilder<T> delete(Class<T> type) {
        return new DefaultDeleteBuilder<T>(type, targetCayenneService, tokenManager, keyAdapterFactory, this, pathNormalizer);
    }

}
