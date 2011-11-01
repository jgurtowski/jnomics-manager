/*
 * Copyright (C) 2011 Matthew A. Titmus
 */

package edu.cshl.schatz.jnomics.mapreduce;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;

import edu.cshl.schatz.jnomics.cli.OptionBuilder;

/**
 * @author Matthew Titmus
 */
public abstract class DistributedBinary extends JnomicsTool {
    public final static String FILE_SEPARATOR = System.getProperty("file.separator");

    /**
     * A {@link Configuration} property: contents will be passed (usually)
     * unmodified to the executed binary.
     */
    public static final String P_BINARY_ARGS = "jnomics.binary.args";

    /**
     * A {@link Configuration} property: Specifies the path(s) to the binary or
     * binaries.
     */
    public static final String P_BINARY_PATH = "jnomics.binary.path";

    public final static String PATH_SEPARATOR = System.getProperty("path.separator");

    private String binaryArgs;

    /**
     * Searches for a file on the current system command path.
     * <p>
     * TODO Determine the command path environment variable by operating system
     * name.
     * 
     * @param filename The name of the file to find.
     * @return A {@link File} object, or <code>null</code> if the file could not
     *         be found.
     */
    public static File findFile(String filename) {
        return findFile(filename, System.getenv("PATH"));
    }

    /**
     * Searches for a file on a specified path.
     * 
     * @param filename The name of the file to find.
     * @param path A string of the directories to search, delimitted by the
     *            system-specific path separator character (as per
     *            <code>{@link System}.getProperty("path.separator")</code>). If
     *            <code>filename</code> contains a path (i.e.,
     *            <code>./file.foo</code>), then the path is ignored.
     * @return A {@link File} object, or <code>null</code> if the file could not
     *         be found.
     */
    public static File findFile(String filename, String path) {
        File file = null;

        // If this is true then the string contains a qualified path, so we only
        // look in that one place.
        if (filename.contains(FILE_SEPARATOR)) {
            file = new File(filename);

            if (!file.exists()) {
                file = null;
            }
        } else {
            File cmd;

            for (String dir : path.split(PATH_SEPARATOR)) {
                if ((cmd = new File(dir, filename)).exists()) {
                    file = cmd;
                    break;
                }
            }
        }

        return file;
    }

    /**
     * @return The binaryArgs
     */
    public String getBinaryArgs() {
        return binaryArgs;
    }

    /**
     * {@inheritDoc}
     * <p>
     * TODO - Add generic support for 'args' parameter.
     * 
     * @see edu.cshl.schatz.jnomics.mapreduce.JnomicsTool#getOptions()
     */
    @Override
    public Options getOptions() {
        OptionBuilder optionBuilder = new OptionBuilder();
        Options options = new Options();

        /** @formatter:off */
        options.addOption(optionBuilder
            .withArgName("str")
            .withDescription("Parameters to pass directly to the binary (enclose in quotes).")
            .hasArg()
            .isRequired(false)
            .create("args")
            );
        /** @formatter:on */

        return options;
    }

    /**
     * @param binaryArgs The binaryArgs to set
     */
    public void setBinaryArgs(String binaryArgs) {
        this.binaryArgs = binaryArgs;
    }

    protected File includeFile(String path) throws FileNotFoundException {
        File binaryFile = findFile(path);

        if (binaryFile == null) {
            String msg = String.format(
                "File %s does not exist in command path (PATH=%s)", path, System.getenv("PATH"));

            throw new FileNotFoundException(msg);
        }

        System.err.println("WARNING: includeFile() not fully implemented!");

        return binaryFile;
    }

    protected static class StreamThread extends Thread {
        private InputStream inFrom;
        private OutputStream outTo;

        public StreamThread(InputStream inFrom, OutputStream outTo) {
            this.inFrom = inFrom;
            this.outTo = outTo;
        }

        @Override
        public void run() {
            int read;
            byte[] bytes = new byte[512];

            // Read the processes' output, and push the raw bytes to the
            // specified output stream.
            try {
                while (-1 != (read = inFrom.read(bytes))) {
                    outTo.write(bytes, 0, read);
                }
            } catch (IOException e) {
                try {
                    outTo.flush();
                } catch (IOException e1) {}

                e.printStackTrace();
            }
        }
    }
}