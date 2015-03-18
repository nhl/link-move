package com.nhl.link.etl.batch;

/**
 * A converter of source objects of the batch to the type understood by the
 * processor. In addition to the actual conversion, the converter allows
 * implementors to reuse target objects between the batches to prevent excessive
 * GC.
 */
public interface BatchConverter<R, S> {

	S createTemplate();

	S fromTemplate(R rawSource, S template);
}
