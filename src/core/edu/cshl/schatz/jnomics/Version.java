/*
 * Copyright (C) 2011 Matthew A. Titmus
 *
 * Last modified: $Date$ (revision $Revision$)
 */

package edu.cshl.schatz.jnomics;

/**
 * A container class for the most recent revision number and date.
 * <p>
 * Jnomics uses a major/minor/patch versioning scheme to indicate the
 * significance of changes between releases: changes are (will be) classified by
 * significance level, and the decision of which sequence to change between
 * releases is based on the significance of the changes from the previous
 * release. The current major revision number is 0; Jnomics will be considered
 * to have reached "version 1" once the first (currently undefined) major
 * milestone has been reached.
 * <p>
 * The current development status is indicated by a one or two character
 * designation after the version number, as follows:
 * <ul>
 * <li>a &ndash alpha
 * <li>b &ndash beta
 * <li>rc &ndash release candidate
 * <li>r &ndash (final) release
 * </ul>
 * 
 * @author Matthew A. Titmus
 */
public final class Version {

    /**
     * The current package version "major revision" number (digit 1)
     */
    public static final String MAJOR = "0";

    /**
     * The current package version "minor revision" number (digit 2)
     */
    public static final String MINOR = "1";

    /**
     * The current package version "patch revision" number (digit 3)
     */
    public static final String PATCH = "1";

    /**
     * The current package development status, indicated by a one or two
     * character designation as follows:
     * <ul>
     * <li>a &ndash alpha
     * <li>b &ndash beta
     * <li>rc &ndash release candidate
     * <li>r &ndash (final) release
     */
    public static final String STATUS = "a";

    /**
     * The current package revision number ($Revision$).
     */
    public static final String REVISION = "12";

    /**
     * The date of the most recent revision ($Date$).
     */
    public static final String REVISION_DATE = "Fri Nov  4 14:11:06 EDT 2011";

    /**
     * The current version of the Jnomics API.
     */
    public static final String VERSION = String.format(
        "%s.%s.%s-%s%s", MAJOR, MINOR, PATCH, STATUS, REVISION);

    private static final String _VERSION_STRING = "Jnomics " + VERSION + " (" + REVISION_DATE + ")";

    public static String getVersion() {
        return _VERSION_STRING;
    }

    public static void main(String[] args) throws Exception {
        String out = _VERSION_STRING;

        if (args.length > 0) {
            if (args[0].equals("-v")) {
                out = VERSION;
            } else if (args[0].equals("-r")) {
                out = REVISION;
            } else if (args[0].equals("-d")) {
                out = REVISION_DATE;
            }
        }

        System.out.println(out);
    }
}
