/*
 * Copyright (C) 2011 Matthew A. Titmus
 * 
 * Last modified: $Date$ (revision $Revision$)
 */

package edu.cshl.schatz.jnomics.test.tools;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;

import edu.cshl.schatz.jnomics.tools.PropertyQuery;
import edu.cshl.schatz.jnomics.tools.PropertyQuery.QueryOptions;

/**
 * @author Matthew Titmus
 */
public class PropertyQueryTest extends TestCase {

    public void testSystemPropertyQuery() throws Exception {
        Configuration conf = new Configuration();

        Assert.assertNull(PropertyQuery.query(conf, "user.home"));

        Assert.assertEquals(
            System.getProperty("user.home"),
            PropertyQuery.query(conf, "user.home", QueryOptions.INCLUDE_VM));
    }
}
