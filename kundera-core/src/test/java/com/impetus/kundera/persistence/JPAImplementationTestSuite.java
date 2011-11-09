package com.impetus.kundera.persistence;

import junit.framework.Test;
import junit.framework.TestSuite;

public class JPAImplementationTestSuite
{

    public static Test suite()
    {
        TestSuite suite = new TestSuite(JPAImplementationTestSuite.class.getName());
        // $JUnit-BEGIN$
        suite.addTestSuite(EntityManagerSessionTest.class);

        // TODO: add more test suites/ test cases here

        // $JUnit-END$
        return suite;
    }

}
