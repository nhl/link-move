package com.nhl.link.move.mapper;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class ByteArrayKeyAdapterTest {
	
	@Test
	public void toFrom() {
		ByteArrayKeyAdapter builder = new ByteArrayKeyAdapter();

		byte[] b1 = new byte[] { 1, 2 };
		Object k1 = builder.toMapKey(b1);
		assertNotEquals(b1, k1);
		assertSame(b1, builder.fromMapKey(k1));
	}

	@Test
	public void testEquals() {
		ByteArrayKeyAdapter builder = new ByteArrayKeyAdapter();

		byte[] b1 = new byte[] { 1, 2 };
		assertEquals(builder.toMapKey(b1), builder.toMapKey(b1));

		byte[] b2 = new byte[] { 1, 2 };
		assertEquals(builder.toMapKey(b2), builder.toMapKey(b1));

		byte[] b3 = new byte[] { 1 };
		assertNotEquals(builder.toMapKey(b3), builder.toMapKey(b1));

		byte[] b4 = new byte[] {};
		assertNotEquals(builder.toMapKey(b4), builder.toMapKey(b1));
	}

	@Test
	public void testHashCode() {
		ByteArrayKeyAdapter builder = new ByteArrayKeyAdapter();

		byte[] b1 = new byte[] { 1, 2 };
		assertEquals(builder.toMapKey(b1).hashCode(), builder.toMapKey(b1).hashCode());

		byte[] b2 = new byte[] { 1, 2 };
		assertEquals(builder.toMapKey(b1).hashCode(), builder.toMapKey(b2).hashCode());

		byte[] b3 = new byte[] { 1 };
		assertNotEquals(builder.toMapKey(b3).hashCode(), builder.toMapKey(b1).hashCode());

		byte[] b4 = new byte[] {};
		assertNotEquals(builder.toMapKey(b4).hashCode(), builder.toMapKey(b1).hashCode());
	}

}
