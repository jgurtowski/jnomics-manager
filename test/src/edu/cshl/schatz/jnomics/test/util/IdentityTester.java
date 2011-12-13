/*
 * Copyright (C) 2011 Matthew A. Titmus
 * 
 * Last modified: $Date$ (revision $Revision$)
 */

package edu.cshl.schatz.jnomics.test.util;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import junit.framework.TestCase;

/**
 * Compares two methods of the same class by looping over all visible
 * zero-parameter methods and comparing their outputs.
 * 
 * @author Matthew Titmus
 */
/**
 * @author Matthew A. Titmus
 */
public class IdentityTester {
    private Object expected, actual;

    private Class<?> klass;

    private Map<String, AssertThat> methodActions = new HashMap<String, AssertThat>();

    /**
     * @param expected
     * @param actual
     * @throws NoSuchMethodException
     * @throws SecurityException
     */
    public IdentityTester(Object expected, Object actual)
            throws SecurityException, NoSuchMethodException {

        this(expected, actual, AssertThat.ASSERT_EQUALS);
    }

    /**
     * @param expected
     * @param actual
     * @throws NoSuchMethodException
     * @throws SecurityException
     */
    public IdentityTester(Object expected, Object actual, AssertThat defaultAssert)
            throws SecurityException, NoSuchMethodException {

        this.expected = expected;
        this.actual = actual;
        klass = expected.getClass();

        TestCase.assertTrue(
            actual.getClass().getName() + " is not a subclass of " + klass.getName(),
            klass.isAssignableFrom(actual.getClass()));

        // TestCase.assertEquals(
        // klass.getName() + " != " + actual.getClass().getName(),
        // expected.getClass(),
        // actual.getClass());

        for (Method method : klass.getMethods()) {
            String mName = method.toGenericString();
            Class<?> mReturnType = method.getReturnType();

            if ((method.getParameterTypes().length == 0)
                    && Modifier.isPublic(method.getModifiers()) && (mReturnType != Void.TYPE)) {

                // Yes, they're hard-coded. Blah.

                if (mName.equals("clone")) {
                    setMethodAction(method, AssertThat.ASSERT_NOT_SAME);
                } else if (mName.equals("hashCode")) {
                    setMethodAction(method, AssertThat.ASSERT_NOT_EQUAL);
                } else {
                    setMethodAction(method, defaultAssert);
                }
            }
        }
    }

    /**
     * @param method
     */
    public AssertThat getMethodAction(Method method)
            throws SecurityException, NoSuchMethodException {

        return methodActions.get(method.getName());
    }

    /**
     * @param methodName The methods basic name (i.e., "toString", or
     *            "hashCode").
     * @throws NoSuchMethodException
     * @throws SecurityException
     */
    public AssertThat getMethodAction(String methodName)
            throws SecurityException, NoSuchMethodException {

        return methodActions.get(methodName);
    }

    /**
     * If the objects are the same class, this method loops over all visible
     * zero-parameter methods and compares their outputs.
     * 
     * @throws NoSuchMethodException
     * @throws SecurityException
     * @throws InvocationTargetException
     * @throws IllegalAccessException
     * @throws IllegalArgumentException
     */
    public void runTests()
            throws SecurityException, NoSuchMethodException, IllegalArgumentException,
            IllegalAccessException, InvocationTargetException {

        for (Entry<String, AssertThat> e : methodActions.entrySet()) {
            String methodName = e.getKey();
            AssertThat action = e.getValue();

            if (action != AssertThat.IGNORE) {
                Method method = klass.getMethod(methodName);

                try {
                    Object expectedReturn = method.invoke(expected);
                    Object actualReturn = method.invoke(actual);

                    String message = String.format(
                        "%s.%s() failed assertion (%s)", klass.getSimpleName(), method.getName(),
                        action.toString());

                    Class<?> returnType = method.getReturnType();
                    if (returnType.isArray() && (action == AssertThat.ASSERT_EQUALS)) {
                        Object[] expArr = (Object[]) expectedReturn;
                        Object[] actArr = (Object[]) actualReturn;

                        doEvaluateReturns(
                            message + " (array size)", expArr.length, actArr.length, action);

                        doEvaluateReturns(
                            message + " (contents)", true, Arrays.equals(expArr, actArr), action);

                    } else {
                        doEvaluateReturns(message, expectedReturn, actualReturn, action);
                    }
                    
                    // } catch (RuntimeException ex) {
                    // String message = String.format(
                    // "Got an exception trying %s.%s(). Stack trace follows:",
                    // klass.getSimpleName(), method.getName(),
                    // action.toString());
                    // RuntimeException re;
                    //
                    // re = new RuntimeException(message, ex);
                    // re.setStackTrace(ex.getStackTrace());
                    //
                    // throw re;
                } catch (InvocationTargetException ex) {
                    Throwable t = (ex.getCause() == null ? ex : ex.getCause());

                    String message = String.format(
                        "%s.%s() threw an exception. Stack trace follows:", klass.getSimpleName(),
                        method.getName(), action.toString());

                    System.err.println(message);
                    t.printStackTrace();
                    System.err.flush();
                }
            }
        }
    }

    /**
     * @param method
     * @param action
     * @throws NoSuchMethodException
     * @throws SecurityException
     * @throws UnsupportedOperationException If the method is invalid (for
     *             example, if it requires parameters).
     */
    public void setMethodAction(Method method, AssertThat action)
            throws SecurityException, NoSuchMethodException {

        if (method.getParameterTypes().length > 0) {
            throw new UnsupportedOperationException("Only zero-parameter methods are supported.");
        }

        if (method.getReturnType().equals(Void.TYPE)) {
            throw new UnsupportedOperationException(
                "Nothing to test from void return type methods.");
        }

        if (!Modifier.isPublic(method.getModifiers())) {
            throw new UnsupportedOperationException("Method is not public");
        }

        methodActions.put(method.getName(), action);
    }

    /**
     * @param methodName The methods basic name (i.e., "toString", or
     *            "hashCode").
     * @param action
     * @throws NoSuchMethodException
     * @throws SecurityException
     */
    public void setMethodAction(String methodName, AssertThat action)
            throws SecurityException, NoSuchMethodException {

        setMethodAction(klass.getMethod(methodName), action);
    }

    /**
     * @param expectedReturn
     * @param actualReturn
     * @param action
     */
    private void doEvaluateReturns(String message, Object expected, Object actual, AssertThat action) {
        try {
            switch (action) {
            case ASSERT_EQUALS:
                TestCase.assertEquals(message, expected, actual);
                break;
            case ASSERT_NOT_EQUAL:
                if (expected == null) {
                    TestCase.assertNotNull(message, actual);
                } else {
                    TestCase.assertFalse(message, expected.equals(actual));
                }
                break;
            case ASSERT_NOT_SAME:
                TestCase.assertNotSame(message, expected, actual);
                break;
            case ASSERT_SAME:
                TestCase.assertSame(message, expected, action);
                break;
            case IGNORE:
                break;
            }
        } catch (RuntimeException re) {
            Class<? extends RuntimeException> exClass = re.getClass();
            RuntimeException rethrow;

            String expectedClassName = (expected == null ? "" : expected.getClass().getSimpleName());
            String actualClassName = (actual == null ? "" : actual.getClass().getSimpleName());
            String newMessage = String.format(
                "Msg=\"%s\"; Expected=%s; Actual=%s", message, expectedClassName, actualClassName);

            try {
                Constructor<? extends RuntimeException> constructor = exClass.getConstructor(String.class);

                rethrow = constructor.newInstance(newMessage);
            } catch (Exception e) {
                newMessage = "Type=" + re.getClass().getSimpleName() + "; " + newMessage;
                rethrow = new RuntimeException(newMessage);

                System.err.println("Warning: Could not instantiate new " + re.getClass()
                        + " via reflection (got " + e.getClass() + ")");
            }

            rethrow.setStackTrace(re.getStackTrace());

            throw rethrow;
        }
    }

    public static enum AssertThat {
        ASSERT_EQUALS,
        ASSERT_NOT_EQUAL,
        ASSERT_NOT_SAME,
        ASSERT_SAME,
        IGNORE
    }
}