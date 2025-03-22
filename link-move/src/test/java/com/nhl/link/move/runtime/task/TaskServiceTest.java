package com.nhl.link.move.runtime.task;

import com.nhl.link.move.DeleteBuilder;
import com.nhl.link.move.log.LmLogger;
import com.nhl.link.move.runtime.cayenne.ITargetCayenneService;
import com.nhl.link.move.runtime.extractor.IExtractorService;
import com.nhl.link.move.runtime.key.IKeyAdapterFactory;
import com.nhl.link.move.runtime.targetmodel.TargetEntityMap;
import com.nhl.link.move.unit.cayenne.t.Etl1t;
import com.nhl.link.move.valueconverter.ValueConverterFactory;
import com.nhl.link.move.writer.ITargetPropertyWriterService;
import org.apache.cayenne.map.EntityResolver;
import org.apache.cayenne.map.ObjEntity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TaskServiceTest {

	private TaskService taskService;

	@BeforeEach
	public void before() {

		ObjEntity targetEntity = mock(ObjEntity.class);

		EntityResolver resolver = mock(EntityResolver.class);
		when(resolver.getObjEntity(any(Class.class))).thenReturn(targetEntity);

		ITargetCayenneService cayenneService = mock(ITargetCayenneService.class);
		when(cayenneService.entityResolver()).thenReturn(resolver);

		IExtractorService extractorService = mock(IExtractorService.class);
		IKeyAdapterFactory keyAdapterFactory = mock(IKeyAdapterFactory.class);

		TargetEntityMap mockPathNormalizer = mock(TargetEntityMap.class);
		ITargetPropertyWriterService writerService = mock(ITargetPropertyWriterService.class);

		taskService = new TaskService(
				extractorService,
				cayenneService,
				keyAdapterFactory,
				mockPathNormalizer,
				writerService,
				mock(ValueConverterFactory.class),
				mock(LmLogger.class));
	}

	@Test
	public void testDelete() {
		DeleteBuilder builder = taskService.delete(Etl1t.class);
		assertNotNull(builder);
		// TODO: anything else we can assert here?
	}
}
