/*
 * Copyright (C) 2011 Matthew A. Titmus
 * 
 * Last modified: $Date$ (revision $Revision$)
 */

package edu.cshl.schatz.jnomics.test.core;

import junit.framework.TestCase;

import org.apache.hadoop.io.Text;

import edu.cshl.schatz.jnomics.util.TextCutter;

/**
 * @author Matthew A. Titmus
 */
public class TextCutterTest extends TestCase {
    // private static final String nullValue = null;
    private static final String emptyString = "";
    private static final String oneCol = "FOO";
    private static final String threeColsDot = "FOO.BAR.BAT";
    private static final String threeColsTab = "FOO\tBAR\tBAT";

    /**
     * TODO Make JUnit 3 compatible (for ANT)
     */
    // @Test(expected = NullPointerException.class)
    // public void test0_null() {
    // TextCutter textCutter = new TextCutter();
    //
    // textCutter.set(null);
    // }

    public void test1_new_cutCount() {
        TextCutter textCutter = new TextCutter();

        assertEquals("Column count != 0", 0, textCutter.getCutCount());
    }

    /**
     * TODO Make JUnit 3 compatible (for ANT)
     */
    // @Test(expected = ArrayIndexOutOfBoundsException.class)
    // public void test1_new_outOfBounds0() {
    // TextCutter textCutter = new TextCutter();
    //
    // textCutter.getColumn(0);
    // }

    /**
     * TODO Make JUnit 3 compatible (for ANT)
     */
    // @Test(expected = ArrayIndexOutOfBoundsException.class)
    // public void test1_new_outOfBounds1() {
    // TextCutter textCutter = new TextCutter();
    // textCutter.getColumn(1);
    // }

    /**
     * TODO Make JUnit 3 compatible (for ANT)
     */
    // @Test

    /**
     * TODO Make JUnit 3 compatible (for ANT)
     */
    // @Test(expected = ArrayIndexOutOfBoundsException.class)
    // public void test1_new_outOfBoundsNeg1() {
    // TextCutter textCutter = new TextCutter();
    //
    // textCutter.getColumn(-1);
    // }

    public void test2_oneCol_cutCount() {
        TextCutter textCutter = new TextCutter();
        Text text = new Text(oneCol);

        textCutter.set(text);
        assertEquals("Column count != 1", 1, textCutter.getCutCount());
    }

    public void test2_oneCol_getCuts() {
        TextCutter textCutter = new TextCutter();
        Text text = new Text(oneCol);

        textCutter.set(text);
        assertEquals("FOO", textCutter.getCut(0).toString());
    }

    /**
     * TODO Make JUnit 3 compatible (for ANT)
     */
    // @Test(expected = ArrayIndexOutOfBoundsException.class)
    // public void test2_oneCol_outOfBounds1() {
    // TextCutter textCutter = new TextCutter();
    // Text text = new Text(oneCol);
    //
    // textCutter.set(text);
    // textCutter.getColumn(1);
    // }

    /**
     * TODO Make JUnit 3 compatible (for ANT)
     */
    // @Test(expected = ArrayIndexOutOfBoundsException.class)
    // public void test2_oneCol_outOfBoundsNeg1() {
    // TextCutter textCutter = new TextCutter();
    // Text text = new Text(oneCol);
    //
    // textCutter.set(text);
    // textCutter.getColumn(-1);
    // }

    public void test3_threeColsTab_cutCount() {
        TextCutter textCutter = new TextCutter();
        Text text = new Text(threeColsTab);

        textCutter.set(text);
        assertEquals("Column count != 3", 3, textCutter.getCutCount());
    }

    public void test3_threeColsTab_getCuts() {
        TextCutter textCutter = new TextCutter();
        Text text = new Text(threeColsTab);

        textCutter.set(text);
        assertEquals("FOO", textCutter.getCut(0).toString());
        assertEquals("BAR", textCutter.getCut(1).toString());
        assertEquals("BAT", textCutter.getCut(2).toString());
    }

    public void test3_threeColsTab_getRange() {
        TextCutter textCutter = new TextCutter();
        Text text = new Text(threeColsTab);

        textCutter.set(text);

        int cutCount = textCutter.getCutCount();
        assertEquals("FOO", textCutter.getCutRange(0, 0).toString());
        assertEquals("FOO\tBAR", textCutter.getCutRange(0, 1).toString());
        assertEquals("FOO\tBAR\tBAT", textCutter.getCutRange(0, 2).toString());
        assertEquals("BAR", textCutter.getCutRange(1, 1).toString());
        assertEquals("BAR\tBAT", textCutter.getCutRange(1, 2).toString());
        assertEquals("BAT", textCutter.getCutRange(2, 2).toString());

        assertEquals("FOO\tBAR\tBAT", textCutter.getCutRange(0, cutCount - 1).toString());
    }

    /**
     * TODO Make JUnit 3 compatible (for ANT)
     */
    // @Test(expected = ArrayIndexOutOfBoundsException.class)
    // public void test3_threeColsTab_outOfBounds3() {
    // TextCutter textCutter = new TextCutter();
    // Text text = new Text(threeColsTab);
    //
    // textCutter.set(text);
    // textCutter.getColumn(3);
    // }

    public void test4_threeColsDot_cutCount() {
        TextCutter textCutter = new TextCutter();
        Text text = new Text(threeColsDot);

        textCutter.set(text);
        textCutter.setDelimiter('.');
        assertEquals("Column count != 3", 3, textCutter.getCutCount());
    }

    public void test4_threeColsDot_getCuts() {
        TextCutter textCutter = new TextCutter();
        Text text = new Text(threeColsDot);

        textCutter.set(text);
        textCutter.setDelimiter('.');
        assertEquals("FOO", textCutter.getCut(0).toString());
        assertEquals("BAR", textCutter.getCut(1).toString());
        assertEquals("BAT", textCutter.getCut(2).toString());
    }

    /**
     * TODO Make JUnit 3 compatible (for ANT)
     */
    // @Test(expected = ArrayIndexOutOfBoundsException.class)
    // public void test4_threeColsDot_outOfBounds3() {
    // TextCutter textCutter = new TextCutter();
    // Text text = new Text(threeColsDot);
    //
    // textCutter.set(text);
    // textCutter.setDelimiter('.');
    // textCutter.getColumn(3);
    // }

    public void test4_threeColsDot2_cutCount() {
        TextCutter textCutter = new TextCutter();
        Text text = new Text(threeColsTab);

        textCutter.set(text);
        textCutter.setDelimiter('.');
        assertEquals("Column count != 1", 1, textCutter.getCutCount());
    }

    public void test4_threeColsDot2_getCuts() {
        TextCutter textCutter = new TextCutter();
        Text text = new Text(threeColsTab);

        textCutter.set(text);
        textCutter.setDelimiter('.');
        assertEquals(threeColsTab, textCutter.getCut(0).toString());
    }

    public void test5_cutRanges() {
        String input = "0 1 2 3 4";
        TextCutter cutter = new TextCutter().set(input).setDelimiter(' ');

        assertEquals("4", cutter.getCut(-1).toString());
        assertEquals("4", cutter.getCutRange(-1, -1).toString());
        assertEquals("2 3 4", cutter.getCutRange(2, 4).toString());
        assertEquals("4 3 2", cutter.getCutRange(4, 2).toString());
        assertEquals("4 3 2", cutter.getCutRange(-1, -3).toString());
    }

    /**
     * TODO Make JUnit 3 compatible (for ANT)
     */
    // @Test(expected = ArrayIndexOutOfBoundsException.class)
    // public void test4_threeColsDot2_outOfBounds() {
    // TextCutter textCutter = new TextCutter();
    // Text text = new Text(threeColsTab);
    //
    // textCutter.set(text);
    // textCutter.setDelimiter('.');
    // textCutter.getColumn(1);
    // }

    public void test5_empty_cutCount() {
        TextCutter textCutter = new TextCutter();
        Text text = new Text(emptyString);

        textCutter.set(text);
        assertEquals("Column count != 1", 1, textCutter.getCutCount());
    }

    public void test5_empty_getCuts() {
        TextCutter textCutter = new TextCutter();
        Text text = new Text(emptyString);

        textCutter.set(text);
        assertEquals(emptyString, textCutter.getCut(0).toString());
    }

    /**
     * TODO Make JUnit 3 compatible (for ANT)
     */
    // @Test(expected = ArrayIndexOutOfBoundsException.class)
    // public void test5_empty_outOfBounds() {
    // TextCutter textCutter = new TextCutter();
    // Text text = new Text(emptyString);
    //
    // textCutter.set(text);
    // textCutter.getColumn(1);
    // }
}
