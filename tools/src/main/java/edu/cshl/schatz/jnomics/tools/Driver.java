/*
 * Copyright (C) 2011 Matthew A. Titmus
 * 
 * Last modified: $Date$ (revision $Revision$)
 */

package edu.cshl.schatz.jnomics.tools;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.ProgramDriver;
import org.apache.hadoop.util.RunJar;

import edu.cshl.schatz.jnomics.Version;

/**
 * Modified from Hadoop's <code>org.apache.hadoop.examples.ExampleDriver</code>
 * class.
 */
public class Driver {
    private static final Log LOG = LogFactory.getLog(Driver.class);

    public static void main(String argv[]) {
        Object javaCommand = System.getProperties().get("sun.java.command");

        if (javaCommand != null
                && !javaCommand.toString().split(" ")[0].equals(RunJar.class.getCanonicalName())) {

            LOG.warn("Jar appear to have been run directly, and may not "
                    + "work correctly. It is recommended that you run this "
                    + "via the 'hadoop' command.");
        }

        MyProgramDriver pgd = new MyProgramDriver();

        try {
            pgd.addClass(
                "guessphred", PhredEncodingGuesser.class,
                "Guess the Phred score encoding scheme in a BAM/SAM file.");

            pgd.addClass(
                AlignmentProcessor.CMD_NAME, AlignmentProcessor.class,
                AlignmentProcessor.CMD_DESCRIPTION);

            pgd.addClass(PropertyQuery.CMD_NAME, PropertyQuery.class, PropertyQuery.CMD_DESCRIPTION);

            pgd.addClass(
                DistributedNovoalign.CMD_NAME, DistributedNovoalign.class,
                DistributedNovoalign.CMD_DESCRIPTION);

            pgd.addClass(
                DistributedBWA.CMD_NAME, DistributedBWA.class, DistributedBWA.CMD_DESCRIPTION);

            pgd.addClass(ConvertCopy.CMD_NAME, ConvertCopy.class, ConvertCopy.CMD_DESCRIPTION);

            pgd.addClass(ReadStats.CMD_NAME, ReadStats.class, ReadStats.CMD_DESCRIPTION);

            pgd.addClass("bowtie2", Bowtie2.class, "Align Reads Bowtie2");

            pgd.addClass("bwajames", BWAJames.class, "BWA James");
            
            pgd.addClass("svsim", SVSimulator.class, "Generate simulated structural variations");

            pgd.addClass("version", Version.class, "Outputs the current version number");

            pgd.addClass("loader_pe", PairedEndLoader.class,"Loads paired end reads into hdfs");

            pgd.addClass("samtools", Samtools.class,"samtools");


        } catch (Throwable e) {
            e.printStackTrace();

            System.exit(1);
        }

        try {
            pgd.driver(argv);
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(1);
        }

        System.exit(0);
    }

    @SuppressWarnings("rawtypes")
    static class MyProgramDriver extends ProgramDriver {
        Set<String> added = new HashSet<String>();

        /*
         * @see org.apache.hadoop.util.ProgramDriver#addClass(java.lang.String,
         * java.lang.Class, java.lang.String)
         */
        @Override
        public void addClass(String name, Class mainClass, String description) throws Throwable {
            super.addClass(name, mainClass, description);

            added.add(mainClass.getCanonicalName());
        }

        public boolean added(Class mainClass) {
            return added.contains(mainClass.getCanonicalName());
        }
    }
}
