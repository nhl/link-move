package com.nhl.link.move.mapper;

public class ByteArrayKeyAdapter implements KeyAdapter {

	@Override
	public Object toMapKey(Object rawKey) {
		return new ByteArrayKey((byte[]) rawKey);
	}

	@Override
	public Object fromMapKey(Object mapKey) {
		return ((ByteArrayKey) mapKey).bytes;
	}

	class ByteArrayKey {

		private byte[] bytes;

		ByteArrayKey(byte[] bytes) {
			this.bytes = bytes;
		}

		@Override
		public boolean equals(Object object) {

			if (object instanceof ByteArrayKey) {
				return equals(bytes, ((ByteArrayKey) object).bytes);
			} else {
				return false;
			}
		}

		@Override
		public int hashCode() {
			int iConstant = 37;
			int iTotal = 17;

			if (bytes == null) {
				return iTotal * iConstant;
			} else {
				for (int i = 0; i < bytes.length; i++) {
					iTotal = iTotal * iConstant + bytes[i];
				}
			}

			return iTotal;
		}

		private boolean equals(byte[] lhs, byte[] rhs) {
			if (lhs == rhs) {
				return true;
			}

			if (lhs == null || rhs == null) {
				return false;
			}

			if (lhs.length != rhs.length) {
				return false;
			}

			for (int i = 0; i < lhs.length; ++i) {
				if (lhs[i] != rhs[i]) {
					return false;
				}
			}

			return true;
		}

	}

}
