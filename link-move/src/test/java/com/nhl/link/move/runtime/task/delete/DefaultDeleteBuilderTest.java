package com.nhl.link.move.runtime.task.delete;

import com.nhl.link.move.annotation.AfterMissingTargetsFiltered;
import com.nhl.link.move.annotation.AfterSourceKeysExtracted;
import com.nhl.link.move.annotation.AfterTargetsMapped;
import com.nhl.link.move.runtime.cayenne.ITargetCayenneService;
import com.nhl.link.move.runtime.extractor.IExtractorService;
import com.nhl.link.move.runtime.key.IKeyAdapterFactory;
import com.nhl.link.move.runtime.targetmodel.TargetEntity;
import com.nhl.link.move.runtime.targetmodel.TargetEntityMap;
import com.nhl.link.move.runtime.task.ITaskService;
import com.nhl.link.move.runtime.task.ListenersBuilder;
import com.nhl.link.move.runtime.task.MapperBuilder;
import com.nhl.link.move.runtime.task.StageListener;
import com.nhl.link.move.runtime.task.sourcekeys.DefaultSourceKeysBuilder;
import com.nhl.link.move.runtime.token.ITokenManager;
import com.nhl.link.move.unit.cayenne.t.Etl1t;
import com.nhl.link.move.valueconverter.ValueConverterFactory;
import org.apache.cayenne.map.EntityResolver;
import org.apache.cayenne.map.ObjAttribute;
import org.apache.cayenne.map.ObjEntity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.annotation.Annotation;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DefaultDeleteBuilderTest {

	private DefaultDeleteBuilder<Etl1t> builder;

	@BeforeEach
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

		TargetEntity mockTargetEntity = mock(TargetEntity.class);
		when(mockTargetEntity.getAttribute(any(String.class))).thenReturn(Optional.empty());

		TargetEntityMap mockPathNormalizer = mock(TargetEntityMap.class);
		when(mockPathNormalizer.get(targetEntity)).thenReturn(mockTargetEntity);

		ITaskService taskService = mock(ITaskService.class);
		when(taskService.extractSourceKeys(Etl1t.class)).thenReturn(
						new DefaultSourceKeysBuilder(
                                mockTargetEntity,
								mock(IExtractorService.class),
								mock(ITokenManager.class),
								keyAdapterFactory,
                                mock(ValueConverterFactory.class)));

		MapperBuilder mapperBuilder = new MapperBuilder(targetEntity, mockTargetEntity, keyAdapterFactory);

		this.builder = new DefaultDeleteBuilder<>(Etl1t.class,
				cayenneService,
				mock(ITokenManager.class),
				taskService,
                mapperBuilder);
	}

	@Test
	public void testTask_NoExtractorName() {
		assertThrows(IllegalStateException.class, () -> builder.task());
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

	public static class DeleteListener1 {

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
