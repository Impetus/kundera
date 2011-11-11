package com.impetus.kundera;

import junit.framework.Test;
import junit.framework.TestSuite;

import com.impetus.kundera.persistence.JPAImplementationTestSuite;

public class AllTestSuites
{

    public static Test suite()
    {
        TestSuite suite = new TestSuite(AllTestSuites.class.getName());
        // $JUnit-BEGIN$

        suite.addTest(JPAImplementationTestSuite.suite());

        // $JUnit-END$
        return suite;
    }

}
