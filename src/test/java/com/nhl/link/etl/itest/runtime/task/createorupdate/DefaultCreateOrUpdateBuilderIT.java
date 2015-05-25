package com.nhl.link.etl.itest.runtime.task.createorupdate;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

import org.junit.Before;
import org.junit.Test;

import com.nhl.link.etl.runtime.cayenne.ITargetCayenneService;
import com.nhl.link.etl.runtime.cayenne.TargetCayenneService;
import com.nhl.link.etl.runtime.extract.IExtractorService;
import com.nhl.link.etl.runtime.key.IKeyAdapterFactory;
import com.nhl.link.etl.runtime.task.createorupdate.DefaultCreateOrUpdateBuilder;
import com.nhl.link.etl.runtime.token.ITokenManager;
import com.nhl.link.etl.unit.DerbySrcTargetTest;
import com.nhl.link.etl.unit.cayenne.t.Etl1t;

public class DefaultCreateOrUpdateBuilderIT extends DerbySrcTargetTest {

	private ITargetCayenneService cayenneService;

	@Before
	public void before() {
		this.cayenneService = new TargetCayenneService(targetStack.runtime());
	}

	@Test(expected = IllegalStateException.class)
	public void testTask_NoExtractorName() {
		DefaultCreateOrUpdateBuilder<Etl1t> builder = new DefaultCreateOrUpdateBuilder<>(Etl1t.class, cayenneService,
				mock(IExtractorService.class), mock(ITokenManager.class), mock(IKeyAdapterFactory.class));

		builder.task();
	}

	@Test
	public void testTask_ExtractorPresent() {
		DefaultCreateOrUpdateBuilder<Etl1t> builder = new DefaultCreateOrUpdateBuilder<>(Etl1t.class, cayenneService,
				mock(IExtractorService.class), mock(ITokenManager.class), mock(IKeyAdapterFactory.class));

		builder.sourceExtractor("test");
		assertNotNull(builder.task());
	}
}
