package com.nhl.link.move.extractor;

import com.nhl.link.move.Execution;
import com.nhl.link.move.RowAttribute;
import com.nhl.link.move.RowReader;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class MultiExtractorRowReader implements RowReader {

    private static final Iterator<Object[]> EMPTY_ITERATOR = new Iterator<>() {

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public Object[] next() {
            throw new NoSuchElementException("No more elements");
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    };

    private final List<Extractor> extractors;
    private final Execution exec;

    private int currentExtractor;
    private RowReader currentReader;
    private Iterator<Object[]> currentIterator;

    public MultiExtractorRowReader(List<Extractor> extractors, Execution exec) {
        this.extractors = extractors;
        this.exec = exec;

        rewind();
    }

    @Override
    public void close() {
        // close last reader if any...
        if (currentReader != null) {
            currentReader.close();
            currentReader = null;
        }
    }

    @Override
    public RowAttribute[] getHeader() {
        return currentReader != null ? currentReader.getHeader() : new RowAttribute[0];
    }

    @Override
    public Iterator<Object[]> iterator() {


        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                return currentIterator().hasNext();
            }

            @Override
            public Object[] next() {
                return currentIterator().next();
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    protected Iterator<Object[]> currentIterator() {

        if (currentIterator == EMPTY_ITERATOR) {
            return currentIterator;
        }

        if (currentIterator == null || !currentIterator.hasNext()) {
            rewind();
        }

        return currentIterator;
    }

    private void rewind() {
        if (currentExtractor >= extractors.size()) {
            currentIterator = EMPTY_ITERATOR;
        } else {

            if (currentReader != null) {
                currentReader.close();
            }

            this.currentReader = extractors.get(currentExtractor++).getReader(exec);
            this.currentIterator = currentReader.iterator();

            // recursion: if currentIterator is empty, we need to continue until
            // we find the one that is not, or exhaust all extractors
            if (!currentIterator.hasNext()) {
                rewind();
            }
        }
    }

}
