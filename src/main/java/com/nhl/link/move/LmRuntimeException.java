package com.nhl.link.move;

public class LmRuntimeException extends RuntimeException {

	private static final long serialVersionUID = -8073322365246551383L;

	public LmRuntimeException() {
		super();
	}

	public LmRuntimeException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public LmRuntimeException(String message, Throwable cause) {
		super(message, cause);
	}

	public LmRuntimeException(String message) {
		super(message);
	}

	public LmRuntimeException(Throwable cause) {
		super(cause);
	}

}
