package com.nhl.link.move.extractor;

import com.nhl.link.move.CollectionRowReader;
import com.nhl.link.move.Execution;
import com.nhl.link.move.RowAttribute;
import com.nhl.link.move.RowReader;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MultiExtractorTest {

    @Test
    public void testGetReader_Empty() {
        MultiExtractor e = new MultiExtractor(Collections.emptyList());
        assertFound(e, 0);
    }

    @Test
    public void testGetReader_One() {
        int[] closeCounter = new int[1];

        List<Extractor> extractors = new ArrayList<>();
        extractors.add(makeExtractor(2, closeCounter));

        MultiExtractor e = new MultiExtractor(extractors);
        assertFound(e, 2);
        assertEquals(1, closeCounter[0]);
    }

    @Test
    public void testGetReader_Many() {

        int[] closeCounter = new int[1];

        List<Extractor> extractors = new ArrayList<>();
        extractors.add(makeExtractor(2, closeCounter));
        extractors.add(makeExtractor(3, closeCounter));
        extractors.add(makeExtractor(4, closeCounter));

        MultiExtractor e = new MultiExtractor(extractors);
        assertFound(e, 9);
        assertEquals(3, closeCounter[0]);
    }

    @Test
    public void testGetReader_Many_FirstEmpty() {

        int[] closeCounter = new int[1];

        List<Extractor> extractors = new ArrayList<>();
        extractors.add(makeExtractor(0, closeCounter));
        extractors.add(makeExtractor(3, closeCounter));
        extractors.add(makeExtractor(4, closeCounter));

        MultiExtractor e = new MultiExtractor(extractors);
        assertFound(e, 7);
        assertEquals(3, closeCounter[0]);
    }

    protected void assertFound(MultiExtractor extractor, int expectedRows) {

        Execution exec = mock(Execution.class);
        when(exec.getParameters()).thenReturn(Collections.emptyMap());

        int rows = 0;
        try (RowReader reader = extractor.getReader(exec)) {
            for (Object[] ignored : reader) {
                rows++;
            }
        }

        assertEquals(expectedRows, rows);

    }

    protected Extractor makeExtractor(int rowsToReturn, int[] closeCounter) {
        return parameters -> {

            RowAttribute[] header = new RowAttribute[0];

            Collection<Object[]> rows = new ArrayList<>(rowsToReturn);
            for (int i = 0; i < rowsToReturn; i++) {
                rows.add(new Object[0]);
            }

            return new CollectionRowReader(header, rows) {
                @Override
                public void close() {
                    closeCounter[0]++;
                }
            };
        };
    }
}
