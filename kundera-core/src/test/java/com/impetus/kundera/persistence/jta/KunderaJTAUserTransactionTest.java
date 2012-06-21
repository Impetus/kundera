package com.impetus.kundera.persistence.jta;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.UserTransaction;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.impetus.kundera.KunderaException;

/**
 * Junit test case for {@link KunderaJTAUserTransaction}. 
 * 
 * @author vivek.mishra
 * 
 */
public class KunderaJTAUserTransactionTest
{
    private UserTransaction utx;

    
    /**
     * Static initialisation and binding UserTransaction.
     * 
     * @throws java.lang.Exception
     */
    @BeforeClass
    public static void setUp() throws Exception
    {
        System.setProperty(Context.INITIAL_CONTEXT_FACTORY, "org.apache.naming.java.javaURLContextFactory");
        System.setProperty(Context.URL_PKG_PREFIXES, "org.apache.naming");

        Context ctx = new InitialContext();
        ctx.createSubcontext("java:comp");
        // This is what we need to bind to get handle of JTA's
        // UserTransaction.

        ctx.bind("java:comp/UserTransaction", new KunderaJTAUserTransaction());

    }

    /**
     * Context lookup on before running each method.
     * 
     * @throws Exception
     */
    @Before
    public void setup() throws Exception
    {
        Context ctx = new InitialContext();
        utx = (UserTransaction) ctx.lookup("java:comp/UserTransaction");

    }

    /**
     * Method to test positive scenario:
     * A) begin transaction.
     * B) assertion on invocation of commit method.
     */
    @Test
    public void testInitializeViaLookup()
    {
        try
        {
            Assert.assertNotNull(utx);
            Assert.assertSame(KunderaJTAUserTransaction.class, utx.getClass());
            utx.begin();
            // pass true, as transaction has already begun.
            assertOnCommit(utx, true);
        }

        catch (NotSupportedException nsuex)
        {
            Assert.fail(nsuex.getMessage());
        }
        catch (SystemException ex)
        {
            Assert.fail(ex.getMessage());
        }
    }

    /**
     * Test case for:
     * A) utx.begin was not invoked.
     * B) Assert on invalid commit.
     */
    @Test
    public void testInvalidCommit()
    {
        try
        {
            Assert.assertNotNull(utx);
            Assert.assertSame(KunderaJTAUserTransaction.class, utx.getClass());
            // utx.begin();
            // pass true, as transaction has already begun.
            assertOnCommit(utx, false);
        }
        catch (SystemException ex)
        {
            Assert.fail(ex.getMessage());
        }
    }

    private void assertOnCommit(UserTransaction utx, boolean isValid) throws SystemException
    {
        try
        {
            utx.commit();
        }
        catch (KunderaException kex)
        {
            if (!isValid)
            {
                Assert.assertSame("No transaction in progress.", kex.getMessage());
            }
            else
            {
                Assert.fail(kex.getMessage());
            }
        }
        catch (SecurityException e)
        {
            Assert.fail(e.getMessage());
        }
        catch (IllegalStateException e)
        {
            Assert.fail(e.getMessage());
        }
        catch (RollbackException e)
        {
            Assert.fail(e.getMessage());
        }
        catch (HeuristicMixedException e)
        {
            Assert.fail(e.getMessage());
        }
        catch (HeuristicRollbackException e)
        {
            Assert.fail(e.getMessage());
        }
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
    }

}
