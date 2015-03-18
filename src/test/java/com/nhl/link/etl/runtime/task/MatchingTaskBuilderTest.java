package com.nhl.link.etl.runtime.task;

import static org.mockito.Mockito.mock;

import org.junit.Test;

import com.nhl.link.etl.runtime.cayenne.ITargetCayenneService;
import com.nhl.link.etl.runtime.extract.IExtractorService;
import com.nhl.link.etl.runtime.mapper.IKeyAdapterFactory;
import com.nhl.link.etl.runtime.token.ITokenManager;
import com.nhl.link.etl.task.createorupdate.CreateOrUpdateTaskBuilder;
import com.nhl.link.etl.unit.cayenne.t.Etl1t;

public class MatchingTaskBuilderTest {

	@Test(expected = IllegalStateException.class)
	public void testTask_NoExtractorName() {
		CreateOrUpdateTaskBuilder<Etl1t> builder = new CreateOrUpdateTaskBuilder<>(Etl1t.class, mock(ITargetCayenneService.class),
				mock(IExtractorService.class), mock(ITokenManager.class), mock(IKeyAdapterFactory.class));

		builder.task();
	}

	@Test(expected = IllegalStateException.class)
	public void testTask_NoMatcher() {
		CreateOrUpdateTaskBuilder<Etl1t> builder = new CreateOrUpdateTaskBuilder<>(Etl1t.class, mock(ITargetCayenneService.class),
				mock(IExtractorService.class), mock(ITokenManager.class), mock(IKeyAdapterFactory.class));
		builder.withExtractor("test");

		builder.task();
	}

}
