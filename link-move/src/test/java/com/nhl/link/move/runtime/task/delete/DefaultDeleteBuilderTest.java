package com.nhl.link.move.runtime.task.delete;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.annotation.Annotation;
import java.util.List;
import java.util.Map;

import org.apache.cayenne.exp.Expression;
import org.apache.cayenne.map.EntityResolver;
import org.apache.cayenne.map.ObjAttribute;
import org.apache.cayenne.map.ObjEntity;
import org.junit.Before;
import org.junit.Test;

import com.nhl.link.move.annotation.AfterMissingTargetsFiltered;
import com.nhl.link.move.annotation.AfterSourceKeysExtracted;
import com.nhl.link.move.annotation.AfterTargetsMapped;
import com.nhl.link.move.runtime.cayenne.ITargetCayenneService;
import com.nhl.link.move.runtime.extractor.IExtractorService;
import com.nhl.link.move.runtime.key.IKeyAdapterFactory;
import com.nhl.link.move.runtime.path.EntityPathNormalizer;
import com.nhl.link.move.runtime.path.IPathNormalizer;
import com.nhl.link.move.runtime.task.ITaskService;
import com.nhl.link.move.runtime.task.ListenersBuilder;
import com.nhl.link.move.runtime.task.StageListener;
import com.nhl.link.move.runtime.task.sourcekeys.DefaultSourceKeysBuilder;
import com.nhl.link.move.runtime.token.ITokenManager;
import com.nhl.link.move.unit.cayenne.t.Etl1t;

public class DefaultDeleteBuilderTest {

	private DefaultDeleteBuilder<Etl1t> builder;

	@Before
	public void before() {

		ObjAttribute matchAttribute = new ObjAttribute("abc");
		matchAttribute.setType(Object.class.getName());

		ObjEntity targetEntity = new ObjEntity();
		targetEntity.addAttribute(matchAttribute);

		EntityResolver resolver = mock(EntityResolver.class);
		when(resolver.getObjEntity(any(Class.class))).thenReturn(targetEntity);

		ITargetCayenneService cayenneService = mock(ITargetCayenneService.class);
		when(cayenneService.entityResolver()).thenReturn(resolver);

		IKeyAdapterFactory keyAdapterFactory = mock(IKeyAdapterFactory.class);

		EntityPathNormalizer mockEntityPathNormalizer = mock(EntityPathNormalizer.class);

		IPathNormalizer mockPathNormalizer = mock(IPathNormalizer.class);
		when(mockPathNormalizer.normalizer(targetEntity)).thenReturn(mockEntityPathNormalizer);

		ITaskService taskService = mock(ITaskService.class);
		when(taskService.extractSourceKeys(Etl1t.class)).thenReturn(new DefaultSourceKeysBuilder(
				mockEntityPathNormalizer, mock(IExtractorService.class), mock(ITokenManager.class), keyAdapterFactory));

		this.builder = new DefaultDeleteBuilder<>(Etl1t.class, cayenneService, keyAdapterFactory, taskService,
				mockPathNormalizer);
	}

	@Test(expected = IllegalStateException.class)
	public void testTask_NoExtractorName() {
		builder.task();
	}

	@Test
	public void test_MinimalConfig() {

		DeleteTask<Etl1t> task = builder.sourceMatchExtractor("x").matchBy("abc").task();
		assertNotNull(task);
		assertEquals(Etl1t.class, task.type);
		assertEquals(500, task.batchSize);
		assertNull(task.targetFilter);
		assertEquals("x", task.extractorName);
	}

	@Test
	public void testTargetFilter() {
		Expression e = Etl1t.NAME.eq("Joe");
		DeleteTask<Etl1t> task = builder.sourceMatchExtractor("x").matchBy("abc").targetFilter(e).task();
		assertSame(e, task.targetFilter);
	}

	@Test
	public void testBatchSize() {
		DeleteTask<Etl1t> task = builder.sourceMatchExtractor("x").matchBy("abc").batchSize(55).task();
		assertEquals(55, task.batchSize);
	}

	@Test
	public void testCreateListenersBuilder() {

		ListenersBuilder listenersBuilder = builder.createListenersBuilder();

		DeleteListener1 l1 = new DeleteListener1();
		DeleteListener2 l2 = new DeleteListener2();
		NotAListener l3 = new NotAListener();

		listenersBuilder.addListener(l1);
		listenersBuilder.addListener(l2);
		listenersBuilder.addListener(l3);

		Map<Class<? extends Annotation>, List<StageListener>> listeners = listenersBuilder.getListeners();

		assertNotNull(listeners.get(AfterTargetsMapped.class));
		assertNotNull(listeners.get(AfterSourceKeysExtracted.class));
		assertNotNull(listeners.get(AfterMissingTargetsFiltered.class));
		
		assertEquals(1, listeners.get(AfterTargetsMapped.class).size());
		assertEquals(2, listeners.get(AfterSourceKeysExtracted.class).size());
		assertEquals(1, listeners.get(AfterMissingTargetsFiltered.class).size());
	}

	public class DeleteListener1 {

		@AfterTargetsMapped
		public void afterTargetsMapped(DeleteSegment<?> s) {

		}

		@AfterSourceKeysExtracted
		public void afterSourceKeysExtracted(DeleteSegment<?> s) {

		}
	}

	public class DeleteListener2 {

		@AfterMissingTargetsFiltered
		public void afterMissingTargetsFiltered(DeleteSegment<?> s) {

		}
		
		@AfterSourceKeysExtracted
		public void afterSourceKeysExtracted(DeleteSegment<?> s) {

		}
	}

	public class NotAListener {

		public void someMethod(DeleteSegment<?> s) {

		}
	}
}
