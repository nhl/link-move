package com.nhl.link.etl.runtime.task.createorupdate;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.cayenne.map.EntityResolver;
import org.apache.cayenne.map.ObjEntity;
import org.junit.Before;
import org.junit.Test;

import com.nhl.link.etl.runtime.cayenne.ITargetCayenneService;
import com.nhl.link.etl.runtime.extract.IExtractorService;
import com.nhl.link.etl.runtime.key.IKeyAdapterFactory;
import com.nhl.link.etl.runtime.token.ITokenManager;
import com.nhl.link.etl.unit.cayenne.t.Etl1t;

public class DefaultCreateOrUpdateBuilderTest {

	private ITargetCayenneService cayenneService;

	@Before
	public void before() {

		EntityResolver resolver = mock(EntityResolver.class);
		when(resolver.getObjEntity(any(Class.class))).thenReturn(mock(ObjEntity.class));

		this.cayenneService = mock(ITargetCayenneService.class);
		when(cayenneService.entityResolver()).thenReturn(resolver);

	}

	@Test(expected = IllegalStateException.class)
	public void testTask_NoExtractorName() {
		DefaultCreateOrUpdateBuilder<Etl1t> builder = new DefaultCreateOrUpdateBuilder<>(Etl1t.class, cayenneService,
				mock(IExtractorService.class), mock(ITokenManager.class), mock(IKeyAdapterFactory.class));

		builder.task();
	}

	@Test(expected = IllegalStateException.class)
	public void testTask_NoMatcher() {
		DefaultCreateOrUpdateBuilder<Etl1t> builder = new DefaultCreateOrUpdateBuilder<>(Etl1t.class, cayenneService,
				mock(IExtractorService.class), mock(ITokenManager.class), mock(IKeyAdapterFactory.class));
		builder.sourceExtractor("test");

		builder.task();
	}
}
