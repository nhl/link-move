package com.nhl.link.move.runtime.task;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.cayenne.map.EntityResolver;
import org.apache.cayenne.map.ObjEntity;
import org.junit.Before;
import org.junit.Test;

import com.nhl.link.move.DeleteBuilder;
import com.nhl.link.move.runtime.cayenne.ITargetCayenneService;
import com.nhl.link.move.runtime.extractor.IExtractorService;
import com.nhl.link.move.runtime.key.IKeyAdapterFactory;
import com.nhl.link.move.runtime.path.IPathNormalizer;
import com.nhl.link.move.runtime.task.TaskService;
import com.nhl.link.move.runtime.token.ITokenManager;
import com.nhl.link.move.unit.cayenne.t.Etl1t;

public class TaskServiceTest {

	private TaskService taskService;

	@Before
	public void before() {

		ObjEntity targetEntity = mock(ObjEntity.class);

		EntityResolver resolver = mock(EntityResolver.class);
		when(resolver.getObjEntity(any(Class.class))).thenReturn(targetEntity);

		ITargetCayenneService cayenneService = mock(ITargetCayenneService.class);
		when(cayenneService.entityResolver()).thenReturn(resolver);

		IExtractorService extractorService = mock(IExtractorService.class);
		ITokenManager tokenManager = mock(ITokenManager.class);
		IKeyAdapterFactory keyAdapterFactory = mock(IKeyAdapterFactory.class);

		IPathNormalizer mockPathNormalizer = mock(IPathNormalizer.class);

		taskService = new TaskService(extractorService, cayenneService, tokenManager, keyAdapterFactory,
				mockPathNormalizer);
	}

	@Test
	public void testDelete() {
		DeleteBuilder<Etl1t> builder = taskService.delete(Etl1t.class);
		assertNotNull(builder);
		// TODO: anything else we can assert here?
	}
}
