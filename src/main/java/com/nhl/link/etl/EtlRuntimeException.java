package com.nhl.link.etl;

public class EtlRuntimeException extends RuntimeException {

	private static final long serialVersionUID = -8073322365246551383L;

	public EtlRuntimeException() {
		super();
	}

	public EtlRuntimeException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public EtlRuntimeException(String message, Throwable cause) {
		super(message, cause);
	}

	public EtlRuntimeException(String message) {
		super(message);
	}

	public EtlRuntimeException(Throwable cause) {
		super(cause);
	}

}
