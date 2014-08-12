package com.nhl.link.framework.etl.batch;

/**
 * A converter that allows to preprocess batch source objects before passing
 * them to targets. In addition to the actual conversion, the converter allows
 * implementors to reuse target objects between the batches to prevent excessive
 * GC.
 */
public interface BatchConverter<S, T> {

	T createTemplate();

	T fromTemplate(S source, T targetTemplate);
}
