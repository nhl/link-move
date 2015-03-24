package com.nhl.link.etl.runtime.task.createorupdate;

import static org.mockito.Mockito.mock;

import org.junit.Test;

import com.nhl.link.etl.runtime.cayenne.ITargetCayenneService;
import com.nhl.link.etl.runtime.extract.IExtractorService;
import com.nhl.link.etl.runtime.mapper.IKeyAdapterFactory;
import com.nhl.link.etl.runtime.token.ITokenManager;
import com.nhl.link.etl.unit.cayenne.t.Etl1t;

public class DefaultCreateOrUpdateBuilderTest {

	@Test(expected = IllegalStateException.class)
	public void testTask_NoExtractorName() {
		DefaultCreateOrUpdateBuilder<Etl1t> builder = new DefaultCreateOrUpdateBuilder<>(Etl1t.class,
				mock(ITargetCayenneService.class), mock(IExtractorService.class), mock(ITokenManager.class),
				mock(IKeyAdapterFactory.class));

		builder.task();
	}

	@Test(expected = IllegalStateException.class)
	public void testTask_NoMatcher() {
		DefaultCreateOrUpdateBuilder<Etl1t> builder = new DefaultCreateOrUpdateBuilder<>(Etl1t.class,
				mock(ITargetCayenneService.class), mock(IExtractorService.class), mock(ITokenManager.class),
				mock(IKeyAdapterFactory.class));
		builder.sourceExtractor("test");

		builder.task();
	}
}
