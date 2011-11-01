/*
 * Copyright (C) 2011 Matthew A. Titmus
 * 
 * Last modified: $Date$ (revision $Revision$)
 */

package edu.cshl.schatz.jnomics.test.core;

import junit.framework.TestCase;
import edu.cshl.schatz.jnomics.ob.PositionRange;

/**
 * @author Matthew Titmus
 */
public class PositionRangeTest extends TestCase {
    public void testOverlap() {
        PositionRange p1, p2;
        int range[];

        // Identical range.
        range = new int[] { 101, 200, 101, 200 };
        p1 = PositionRange.instanceByEnds(range[0], range[1]);
        p2 = PositionRange.instanceByEnds(range[2], range[3]);
        p1.overlap(p2);
        assertEquals(100, p2.length());
        assertEquals(101, p2.first());
        assertEquals(200, p2.last());

        // One range entirely within another
        range = new int[] { 1, 200, 41, 100 };
        p1 = PositionRange.instanceByEnds(range[0], range[1]);
        p2 = PositionRange.instanceByEnds(range[2], range[3]);
        p1.overlap(p2);
        assertEquals(60, p2.length());
        assertEquals(41, p2.first());
        assertEquals(100, p2.last());

        range = new int[] { 41, 100, 1, 200 };
        p1 = PositionRange.instanceByEnds(range[0], range[1]);
        p2 = PositionRange.instanceByEnds(range[2], range[3]);
        p1.overlap(p2);
        assertEquals(60, p2.length());
        assertEquals(41, p2.first());
        assertEquals(100, p2.last());

        // One range partially overlaps another
        range = new int[] { 1, 100, 41, 200 };
        p1 = PositionRange.instanceByEnds(range[0], range[1]);
        p2 = PositionRange.instanceByEnds(range[2], range[3]);
        p1.overlap(p2);
        assertEquals(60, p2.length());
        assertEquals(41, p2.first());
        assertEquals(100, p2.last());

        range = new int[] { 41, 200, 1, 100 };
        p1 = PositionRange.instanceByEnds(range[0], range[1]);
        p2 = PositionRange.instanceByEnds(range[2], range[3]);
        p1.overlap(p2);
        assertEquals(60, p2.length());
        assertEquals(41, p2.first());
        assertEquals(100, p2.last());

        // Ranges touch but don't overlap
        range = new int[] { 1, 100, 101, 200 };
        p1 = PositionRange.instanceByEnds(range[0], range[1]);
        p2 = PositionRange.instanceByEnds(range[2], range[3]);
        p1.overlap(p2);
        assertEquals(0, p2.length());

        range = new int[] { 101, 200, 1, 100 };
        p1 = PositionRange.instanceByEnds(range[0], range[1]);
        p2 = PositionRange.instanceByEnds(range[2], range[3]);
        p1.overlap(p2);
        assertEquals(0, p2.length());

        // Ranges do not overlap
        range = new int[] { 1, 40, 42, 200 };
        p1 = PositionRange.instanceByEnds(range[0], range[1]);
        p2 = PositionRange.instanceByEnds(range[2], range[3]);
        p1.overlap(p2);
        assertEquals(-1, p2.length());

        range = new int[] { 42, 200, 1, 40 };
        p1 = PositionRange.instanceByEnds(range[0], range[1]);
        p2 = PositionRange.instanceByEnds(range[2], range[3]);
        p1.overlap(p2);
        assertEquals(-1, p2.length());

        range = new int[] { 1, 1000, 2001, 4000 };
        p1 = PositionRange.instanceByEnds(range[0], range[1]);
        p2 = PositionRange.instanceByEnds(range[2], range[3]);
        p1.overlap(p2);
        assertEquals(-1000, p2.length());
    }

    public void testSAM() {}
}
