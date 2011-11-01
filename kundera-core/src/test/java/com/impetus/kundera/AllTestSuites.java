package com.impetus.kundera;

import com.impetus.kundera.persistence.JPAImplementationTestSuite;

import junit.framework.Test;
import junit.framework.TestSuite;

public class AllTestSuites
{

    public static Test suite()
    {
        TestSuite suite = new TestSuite(AllTestSuites.class.getName());
        //$JUnit-BEGIN$
        
        suite.addTest(JPAImplementationTestSuite.suite());
        

        //$JUnit-END$
        return suite;
    }

}
