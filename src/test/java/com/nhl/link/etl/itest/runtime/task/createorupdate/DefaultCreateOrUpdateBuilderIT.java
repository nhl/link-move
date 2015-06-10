package com.nhl.link.etl.itest.runtime.task.createorupdate;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.cayenne.map.ObjEntity;
import org.junit.Before;
import org.junit.Test;

import com.nhl.link.etl.runtime.cayenne.ITargetCayenneService;
import com.nhl.link.etl.runtime.cayenne.TargetCayenneService;
import com.nhl.link.etl.runtime.extractor.IExtractorService;
import com.nhl.link.etl.runtime.key.IKeyAdapterFactory;
import com.nhl.link.etl.runtime.path.EntityPathNormalizer;
import com.nhl.link.etl.runtime.path.IPathNormalizer;
import com.nhl.link.etl.runtime.task.createorupdate.DefaultCreateOrUpdateBuilder;
import com.nhl.link.etl.runtime.token.ITokenManager;
import com.nhl.link.etl.unit.DerbySrcTargetTest;
import com.nhl.link.etl.unit.cayenne.t.Etl1t;

public class DefaultCreateOrUpdateBuilderIT extends DerbySrcTargetTest {

	private ITargetCayenneService cayenneService;
	private IPathNormalizer mockPathNormalizer;

	@Before
	public void before() {
		this.cayenneService = new TargetCayenneService(targetStack.runtime());

		EntityPathNormalizer mockEntityPathNormalizer = mock(EntityPathNormalizer.class);

		this.mockPathNormalizer = mock(IPathNormalizer.class);
		when(mockPathNormalizer.normalizer(any(ObjEntity.class))).thenReturn(mockEntityPathNormalizer);
	}

	@Test(expected = IllegalStateException.class)
	public void testTask_NoExtractorName() {
		DefaultCreateOrUpdateBuilder<Etl1t> builder = new DefaultCreateOrUpdateBuilder<>(Etl1t.class, cayenneService,
				mock(IExtractorService.class), mock(ITokenManager.class), mock(IKeyAdapterFactory.class),
				mockPathNormalizer);

		builder.task();
	}

	@Test
	public void testTask_ExtractorPresent() {
		DefaultCreateOrUpdateBuilder<Etl1t> builder = new DefaultCreateOrUpdateBuilder<>(Etl1t.class, cayenneService,
				mock(IExtractorService.class), mock(ITokenManager.class), mock(IKeyAdapterFactory.class),
				mockPathNormalizer);

		builder.sourceExtractor("test");
		assertNotNull(builder.task());
	}
}
