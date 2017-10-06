package com.nhl.link.move.itest.runtime.task.createorupdate;

import com.nhl.link.move.runtime.cayenne.ITargetCayenneService;
import com.nhl.link.move.runtime.cayenne.TargetCayenneService;
import com.nhl.link.move.runtime.extractor.IExtractorService;
import com.nhl.link.move.runtime.key.IKeyAdapterFactory;
import com.nhl.link.move.runtime.path.EntityPathNormalizer;
import com.nhl.link.move.runtime.path.IPathNormalizer;
import com.nhl.link.move.runtime.task.createorupdate.DefaultCreateOrUpdateBuilder;
import com.nhl.link.move.runtime.token.ITokenManager;
import com.nhl.link.move.unit.DerbySrcTargetTest;
import com.nhl.link.move.unit.cayenne.t.Etl1t;
import com.nhl.link.move.writer.ITargetPropertyWriterService;
import org.apache.cayenne.map.ObjEntity;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
				mockPathNormalizer, mock(ITargetPropertyWriterService.class));

		builder.task();
	}

	@Test
	public void testTask_ExtractorPresent() {
		DefaultCreateOrUpdateBuilder<Etl1t> builder = new DefaultCreateOrUpdateBuilder<>(Etl1t.class, cayenneService,
				mock(IExtractorService.class), mock(ITokenManager.class), mock(IKeyAdapterFactory.class),
				mockPathNormalizer, mock(ITargetPropertyWriterService.class));

		builder.sourceExtractor("test.xml");
		assertNotNull(builder.task());
	}
}
