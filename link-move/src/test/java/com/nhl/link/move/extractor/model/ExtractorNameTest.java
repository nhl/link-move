package com.nhl.link.move.extractor.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class ExtractorNameTest {

	@Test
	public void testEquals() {

		ExtractorName n1 = ExtractorName.create("l1", "n1");
		ExtractorName n2 = ExtractorName.create("l2", "n1");
		ExtractorName n3 = ExtractorName.create("l1", "n3");
		ExtractorName n4 = ExtractorName.create("l2", "n2");
		ExtractorName n5 = ExtractorName.create("l1", "n1");

		assertNotEquals(n1, n2);
		assertNotEquals(n1, n3);
		assertNotEquals(n1, n4);
		assertEquals(n1, n5);
	}
	
	@Test
	public void testHashCode() {

		ExtractorName n1 = ExtractorName.create("l1", "n1");
		ExtractorName n2 = ExtractorName.create("l2", "n1");
		ExtractorName n3 = ExtractorName.create("l1", "n3");
		ExtractorName n4 = ExtractorName.create("l2", "n2");
		ExtractorName n5 = ExtractorName.create("l1", "n1");

		assertNotEquals(n1.hashCode(), n2.hashCode());
		assertNotEquals(n1.hashCode(), n3.hashCode());
		assertNotEquals(n1.hashCode(), n4.hashCode());
		assertEquals(n1.hashCode(), n5.hashCode());
	}
}
