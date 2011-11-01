/*
 * Copyright (C) 2011 Matthew A. Titmus
 * 
 * Last modified: $Date$ (revision $Revision$)
 */

package edu.cshl.schatz.jnomics.test.core;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * @author Matthew Titmus
 */
public class FileSystemTest {
    public static void main(String[] args) throws IOException {
        CommandLine cmd;
        GenericOptionsParser parser = new GenericOptionsParser(new Options(), args);

        if (null == (cmd = parser.getCommandLine())) {
            args = new String[0];
        } else {
            args = cmd.getArgs();
        }

        if (args.length == 0) {
            System.err.println("Expected path arguments. Got none.");
            System.exit(1);
        } else {
            try {
                Configuration conf = new Configuration();

                FileSystem fs;
                Path p;
                String outBodyFormat;
                int len = 0;

                // 1st pass is just to determine formatting sizes.

                for (String arg : args) {
                    if (arg.length() > len) {
                        len = arg.length();
                    }
                }

                outBodyFormat = "%-" + (len + 15) + "s \t" + "%4s\t%s";

                System.out.printf(
                    "%-3s   %-" + (len + 7) + "s%s%n", "Pos", "Arg",
                    String.format(outBodyFormat, "URI", "f/d", "Exists?"));

                FileStatus status;
                String pathString, outBody, url;

                for (int i = 0; i < args.length; i++) {
                    pathString = args[i];

                    p = new Path(pathString);

                    try {
                        fs = FileSystem.get(p.toUri(), conf);
                        url = p.toUri().toString();

                        status = fs.getFileStatus(p);

                        if ((fs instanceof LocalFileSystem) && !url.startsWith("file:/")) {
                            url = "file://" + url;
                        }

                        outBody = String.format(
                            outBodyFormat, url, status.isDir() ? "dir" : "file", fs.exists(p)
                                    ? "YES"
                                    : "NO ");
                    } catch (FileNotFoundException e) {
                        url = p.toUri().toString();

                        outBody = String.format(outBodyFormat, url, "?  ", "NO ");
                    } catch (Exception e) {
                        outBody = String.format(
                            outBodyFormat, "Error: " + e.getLocalizedMessage(), "-", "-");
                    }

                    System.out.printf("%3s   %-" + (len + 7) + "s%s%n", i + 1, pathString, outBody);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
