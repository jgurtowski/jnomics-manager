/*
 * Copyright (C) 2011 Matthew A. Titmus
 * 
 * Last modified: $Date$ (revision $Revision$)
 */

package edu.cshl.schatz.jnomics.test.core;

import java.io.IOException;
import java.util.regex.Pattern;

import junit.framework.TestCase;
import edu.cshl.schatz.jnomics.tools.AlignmentProcessor;

/**
 * Known bugs: back to back wild-cards don't always convert property
 * ("BILLIEHOLIDAY_0011:1:??:*:*#0" should get
 * "BILLIEHOLIDAY_0011:1:..:.*.:.*#0", actually gets
 * "BILLIEHOLIDAY_0011:1:.\?:.*:.*#0").
 * 
 * @author Matthew Titmus
 */
public class ReadNameToRegexTest extends TestCase {
    public void testCombined() throws IOException {
        String patterns[] = {
                "gi|49175990:1-10000_1_499_1:0:0_0:0:0_77a6",
                "gi|49175990:1-10000_1017_1483_1:0:0_2:0:0_6?47",
                "gi|49175990:1-10000_1017_1522_1:0:0_1:0:0_1d*",
                "gi|49175990:1-10000_1005_1508_0:0:0_1:0:0_a2[e9]4",
        // "BILLIEHOLIDAY_0011:1:??:*:*#0"
        };

        String testMatches[] = {
                "gi|49175990:1-10000_1_499_1:0:0_0:0:0_77a6",
                "gi|49175990:1-10000_1017_1483_1:0:0_2:0:0_6247",
                "gi|49175990:1-10000_1017_1483_1:0:0_2:0:0_6447",
                "gi|49175990:1-10000_1017_1483_1:0:0_2:0:0_6b47",
                "gi|49175990:1-10000_1017_1522_1:0:0_1:0:0_1d",
                "gi|49175990:1-10000_1017_1522_1:0:0_1:0:0_1da",
                "gi|49175990:1-10000_1017_1522_1:0:0_1:0:0_1dabc",
                "gi|49175990:1-10000_1005_1508_0:0:0_1:0:0_a2e4",
                "gi|49175990:1-10000_1005_1508_0:0:0_1:0:0_a294",
        // "BILLIEHOLIDAY_0011:1:15:3347:8022#0"
        };

        String testNonMatches[] = {
                "gi|49175990:1-10000_1_499_1:0:0_0:0:0_77a61",
                "gi|49175990:1-10000_1017_1483_1:0:0_2:0:0_6257",
                "gi|49175990:1-10000_1017_1522_1:0:0_1:0:0_1c",
                "gi|49175990:1-10000_1017_1522_1:0:0_1:0:0_1ca",
                "gi|49175990:1-10000_1017_1522_1:0:0_1:0:0_1cabc",
                "gi|49175990:1-10000_1005_1508_0:0:0_1:0:0_a254",
        // "BILLIEHOLIDAY_0011:1:105:1215:6457#0"
        };

        for (int i = 0; i < patterns.length; i++) {
            patterns[i] = AlignmentProcessor.NameToRegexConverter.convert(patterns[i]);
        }

        StringBuilder b = new StringBuilder("^((" + patterns[0] + ")");
        for (int i = 1; i < patterns.length; i++) {
            b.append("|(" + patterns[i] + ")");
        }
        b.append(")$");

        Pattern p = Pattern.compile(b.toString());

        for (String s : testMatches) {
            assertTrue(s, p.matcher(s).matches());
        }

        for (String s : testNonMatches) {
            assertFalse(s, p.matcher(s).matches());
        }
    }

    public void testConvert() throws IOException {
        String tests[][] = {
                { "", "" },
                { "No change", "No change" },
                { "wild?card", "wild.card" },
                { "wild*card", "wild.*card" },
                { "wild-c[oa]rd", "wild-c[oa]rd" },
                { "No\\? and yes?", "No\\\\\\? and yes." },
                { "+++++", "\\+\\+\\+\\+\\+" },
                { "\\+\\+\\+\\+\\+", "\\\\\\+\\\\\\+\\\\\\+\\\\\\+\\\\\\+" },
                { "^+++++$", "\\^\\+\\+\\+\\+\\+\\$" },
                { "\\? ? \\+ +", "\\\\\\? . \\\\\\+ \\+" },
                { "[]^$\\+(){}*.?", "[]\\^\\$\\\\\\+\\(\\)\\{\\}.*\\.." },
        // { "BILLIEHOLIDAY_0011:1:??:*:*#0", "BILLIEHOLIDAY_0011:1:..:.*.:.*#0"
        // },
        };

        for (String[] test : tests) {
            String result = AlignmentProcessor.NameToRegexConverter.convert(test[0]);

            System.out.println("\"" + test[0] + "\"" + " --> " + "\"" + result + "\"");
            assertEquals(test[1], result);
        }
    }
}
