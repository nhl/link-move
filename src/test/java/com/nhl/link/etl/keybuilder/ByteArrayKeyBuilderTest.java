package com.nhl.link.etl.keybuilder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.nhl.link.etl.keybuilder.ByteArrayKeyBuilder;

public class ByteArrayKeyBuilderTest {

	@Test
	public void testEquals() {
		ByteArrayKeyBuilder builder = new ByteArrayKeyBuilder();

		byte[] b1 = new byte[] { 1, 2 };
		assertTrue(builder.toKey(b1).equals(builder.toKey(b1)));

		byte[] b2 = new byte[] { 1, 2 };
		assertTrue(builder.toKey(b1).equals(builder.toKey(b2)));

		byte[] b3 = new byte[] { 1 };
		assertFalse(builder.toKey(b1).equals(builder.toKey(b3)));

		byte[] b4 = new byte[] {};
		assertFalse(builder.toKey(b1).equals(builder.toKey(b4)));
	}

	@Test
	public void testHashCode() {
		ByteArrayKeyBuilder builder = new ByteArrayKeyBuilder();

		byte[] b1 = new byte[] { 1, 2 };
		assertEquals(builder.toKey(b1).hashCode(), builder.toKey(b1).hashCode());

		byte[] b2 = new byte[] { 1, 2 };
		assertEquals(builder.toKey(b1).hashCode(), builder.toKey(b2).hashCode());

		byte[] b3 = new byte[] { 1 };
		assertFalse(builder.toKey(b1).hashCode() == builder.toKey(b3).hashCode());

		byte[] b4 = new byte[] {};
		assertFalse(builder.toKey(b1).hashCode() == builder.toKey(b4).hashCode());
	}

}
