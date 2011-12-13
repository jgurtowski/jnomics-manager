/*
 * This file is part of Jnomics.test.
 * Copyright 2011 Matthew A. Titmus
 * All rights reserved.
 *  
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *       
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *       
 *     * Neither the name of the Cold Spring Harbor Laboratory nor the names of
 *       its contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package edu.cshl.schatz.jnomics.test.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import edu.cshl.schatz.jnomics.mapreduce.DistributedBinary;
import edu.cshl.schatz.jnomics.mapreduce.JnomicsJob;

/**
 * Can be used during local testing (such as when running in standalone mode in
 * an IDE) to get a {@link Configuration} instance that points to a locally
 * running HDFS instance.
 * 
 * @author Matthew A. Titmus
 */
public class UserConfigurationHelper {
    public static final String P_FS_DEFAULT_NAME = "fs.default.name";

    /**
     * Find the configuration files associated with the 'hadoop' command
     * specified on the command path, and uses them to generate a
     * {@link Configuration} instance. 
     * 
     * @return A {@link Configuration} instance.
     * @throws FileNotFoundException if the hadoop command can't be found on the
     *             command path.
     * @throws IOException There is an underlying I/O exception loading the
     *             configuration information.
     */
    public static Configuration getUserConfiguration() throws IOException {
        Configuration conf = new Configuration();

        if (conf.get(P_FS_DEFAULT_NAME).startsWith("file:/")) {
            // We seek the configuration files by first locating the 'hadoop'
            // command in the command line, and from there locating its config
            // directory.
            Path cmdPath = new DistributedBinary() {}.findLocalCommand("hadoop");

            if (cmdPath != null) {
                File cmd = new File(cmdPath.toUri()).getCanonicalFile();
                File confDir = new File(cmd.getParentFile().getParentFile(), "conf");

                // Use the config files to create a distributed config.
                File[] confFiles = confDir.listFiles(new FilenameFilter() {
                    @Override
                    public boolean accept(File dir, String name) {
                        return name.endsWith(".xml");
                    }
                });

                for (File f : confFiles) {
                    conf.addResource(f.toURI().toURL());
                }

                conf = new JnomicsJob(conf).getConfiguration();
            } else {
                throw new FileNotFoundException("Could not find hadoop command on command path");
            }
        }

        return conf;
    }
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Configuration confMagic = getUserConfiguration();

        System.out.println("Default: " + conf.get(P_FS_DEFAULT_NAME));
        System.out.println("Magic: " + confMagic.get(P_FS_DEFAULT_NAME));
    }
}
