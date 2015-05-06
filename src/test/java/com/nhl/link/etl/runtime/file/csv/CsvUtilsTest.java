package com.nhl.link.etl.runtime.file.csv;

import static org.junit.Assert.*;
import org.junit.Test;

public class CsvUtilsTest {

    @Test
    public void testParsing1() throws Exception {
        String s1 = "abc,def,ghi";
        String[] result = new String[3];
        CsvUtils.parse(result, s1, ',');
        assertArrayEquals(new String[] {"abc","def","ghi"}, result);
    }

    @Test
    public void testParsing2() throws Exception {
        String s1 = "\"abc\",\"def\",\"ghi\"";
        String[] result = new String[3];
        CsvUtils.parse(result, s1, ',');
        assertArrayEquals(new String[] {"abc","def","ghi"}, result);
    }

    @Test(expected = Exception.class)
    public void testParsing3() throws Exception {
        String s1 = "\"a,b\"c\",\"def\",\"ghi\"";
        String[] result = new String[3];
        CsvUtils.parse(result, s1, ',');
    }

    @Test
    public void testParsing4() throws Exception {
        String s1 = "ab c, \"def\",\"g\"\"hi\"";
        String[] result = new String[3];
        CsvUtils.parse(result, s1, ',');
        assertArrayEquals(new String[] {"ab c","def","g\"hi"}, result);
    }

    @Test
    public void testParsing5() throws Exception {
        String s1 = "a b  c  ,  \"def\" ,\"g\"\"hi\"";
        String[] result = new String[3];
        CsvUtils.parse(result, s1, ',');
        assertArrayEquals(new String[] {"a b  c  ","def","g\"hi"}, result);
    }

    @Test
    public void testParsing6() throws Exception {
        String s1 = "   \"ab c\"   ,  \"def\"  , ghi ";
        String[] result = new String[3];
        CsvUtils.parse(result, s1, ',');
        assertArrayEquals(new String[] {"ab c","def"," ghi "}, result);
    }

    @Test(expected = Exception.class)
    public void testParsing7() throws Exception {
        String s1 = "\"a,b\" \"c\",\"def\",\"ghi\"";
        String[] result = new String[3];
        CsvUtils.parse(result, s1, ',');
    }

}
