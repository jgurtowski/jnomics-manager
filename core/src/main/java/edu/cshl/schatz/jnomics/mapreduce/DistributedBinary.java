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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

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
     * 
     * @param filename The name of the file to find.
     * @return A {@link File} object, or <code>null</code> if the file could not
     *         be found.
     */
    public Path findLocalCommand(String filename) {
        return findLocalFile(filename, System.getenv("PATH"));
    }

    /**
     * Searches for a file on a specified path.
     * 
     * @param filename The name of the file to find.
     * @param searchPath A string of the directories to search, delimited by
     *            the system-specific path separator character (as per
     *            <code>{@link System}.getProperty("path.separator")</code>). If
     *            <code>filename</code> contains a path (i.e.,
     *            <code>./file.foo</code>), then the path is ignored.
     * @return A {@link File} object, or <code>null</code> if the file could not
     *         be found.
     */
    public Path findLocalFile(String filename, String searchPath) {
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

            for (String dir : searchPath.split(PATH_SEPARATOR)) {
                if ((cmd = new File(dir, filename)).exists()) {
                    file = cmd;
                    break;
                }
            }
        }

        return new Path(file.getAbsoluteFile().toURI().toString());
    }

    // /**
    // * Adds an archive specified by a URI to the distributed file system
    // * (default file system is local).
    // *
    // * @param uri
    // * @return
    // * @throws IOException
    // * @throws FileNotFoundException
    // */
    // protected Path distributeArchiveFile(String uri) throws IOException {
    // Path path;
    // FileSystem fs;
    //
    // // First, search for the file on the local file system
    // File localFile = findLocalFile(uri);
    // if (localFile != null) {
    // fs = FileSystem.getLocal(getConf()); // path.getFileSystem(getConf());
    // path = new Path(localFile.getAbsolutePath());
    // } else {
    // // If we got here, then we couldn't find the file on the local file
    // // system. Check against the default fs.
    //
    // path = new Path(uri);
    // fs = FileSystem.get(path.toUri(), getConf());
    // }
    //
    // if (!fs.exists(path)) {
    // String msg = String.format(
    // "File %s does not exist in command path (PATH=%s)", path,
    // System.getenv("PATH"));
    //
    // throw new FileNotFoundException(msg);
    // }
    //
    // path = fs.makeQualified(path);
    // System.out.println(path);
    //
    // DistributedCache.addCacheArchive(path.toUri(), getConf());
    //
    // return path;
    // }

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

    /**
     * Copies a specified local file to the HDFS at
     * <code>/$HDFS_HOME/dist/</code>. If the file already exists at the
     * destination path and has a timestamp >= the local timestamp, then this
     * method will exit and nothing will be copied. The source file is not
     * removed.
     * 
     * @param pathLocal The local file to be copied.
     * @return The {@link Path} at which the file was uploaded.
     * @throws FileNotFoundException If the source path doesn't exist.
     */
    protected Path distributeIfNew(Path pathLocal) throws IOException {
        return distributeIfNew(pathLocal, null);
    }

    /**
     * Copies a specified local file to the HDFS. If the file already exists at
     * the destination path and has a timestamp >= the local timestamp, then
     * this method will exit and nothing will be copied. The source file is not
     * removed.
     * 
     * @param pathLocal The local file to be copied.
     * @param pathDfs The path to the copy destination. If this is a directory,
     *            then the file will be copied into it. If <code>null</code>,
     *            then this defaults to <code>/$HDFS_HOME/dist/</code>.
     * @return The {@link Path} at which the file was uploaded.
     * @throws FileNotFoundException If the source path doesn't exist.
     */
    protected Path distributeIfNew(Path pathLocal, Path pathDfs) throws IOException {
        FileSystem fsDfs, fsLocal = FileSystem.getLocal(getConf());
        long timestampLocal, timestampDfs = 0;

        if (fsLocal.exists(pathLocal)) {
            timestampLocal = fsLocal.getFileStatus(pathLocal).getModificationTime();
        } else {
            throw new FileNotFoundException(pathLocal.toString());
        }

        // If the destination path is null, we set it to "/user/$USER/dist/"
        if (pathDfs == null) {
            fsDfs = FileSystem.get(getConf());
            pathDfs = new Path("dist");

            if (!fsDfs.exists(pathDfs)) {
                fsDfs.mkdirs(pathDfs);
            }
        } else {
            fsDfs = FileSystem.get(pathDfs.toUri(), getConf());
        }

        if (fsDfs.exists(pathDfs)) {
            // Is the target a directory? If so, set the target path to
            // "$path/$binName"
            if (fsDfs.getFileStatus(pathDfs).isDir()) {
                pathDfs = new Path(pathDfs, pathLocal.getName());
            }

            timestampDfs = fsDfs.getFileStatus(pathDfs).getModificationTime();
        }

        Path qualified = fsDfs.makeQualified(pathDfs);
        System.out.printf(
            "DFS Path: %s (Exists=%s)%n", qualified.toString(), fsDfs.exists(qualified));

        // If the local copy is newer than the DFS copy, push it to the DFS.
        if (timestampLocal > timestampDfs) {
            fsDfs.copyFromLocalFile(false, true, pathLocal, pathDfs);
            fsDfs.setTimes(pathDfs, timestampLocal, timestampLocal);
        }

        // DistributedCache.addCacheFile(pathDfs.toUri(), getConf());

        return pathDfs;
    }

    /**
     * Copies a specified local file to the HDFS. If the file already exists at
     * the destination path and has a timestamp >= the local timestamp, then
     * this method will exit and nothing will be copied. The source file is not
     * removed.
     * 
     * @param pathLocal The local file to be copied.
     * @param pathDfs The path to the copy destination. If this is a directory,
     *            then the file will be copied into it. If <code>null</code>,
     *            then this defaults to <code>/$HDFS_HOME/dist/</code>.
     * @return The {@link Path} at which the file was uploaded.
     * @throws FileNotFoundException If the source path doesn't exist.
     */
    protected Path distributeIfNew(String pathLocal, String pathDfs) throws IOException {
        return distributeIfNew(new Path(pathLocal), pathDfs == null ? null : new Path(pathDfs));
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