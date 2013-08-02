/*******************************************************************************
 * * Copyright 2012 Impetus Infotech.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 ******************************************************************************/
package com.impetus.kundera.persistence.jta;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.Status;
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
     * Method to test positive scenario: A) begin transaction. B) assertion on
     * invocation of commit method.
     */
    @Test
    public void testInitializeViaLookup()
    {
        try
        {
            Assert.assertNotNull(utx);
            Assert.assertSame(KunderaJTAUserTransaction.class, utx.getClass());
            utx.begin();
            Assert.assertEquals(Status.STATUS_ACTIVE, utx.getStatus());
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
     * Test case for: A) utx.begin was not invoked. B) Assert on invalid commit.
     */
    @Test
    public void testInvalidCommit()
    {
        try
        {
            Assert.assertNotNull(utx);
            Assert.assertSame(KunderaJTAUserTransaction.class, utx.getClass());
            // pass false, as transaction has not begun.
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
            Assert.assertEquals(Status.STATUS_NO_TRANSACTION, utx.getStatus());
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
     * Method to test rollback
     */
    @Test
    public void testRollback()
    {
        try
        {
            Assert.assertNotNull(utx);
            Assert.assertSame(KunderaJTAUserTransaction.class, utx.getClass());

            // Rollback without begin of transaction.
            try
            {
                utx.rollback();
                Assert.fail("Should have gone to catch block1");
            }
            catch (IllegalStateException isex)
            {
                Assert.assertEquals("Cannot locate a Transaction for rollback.", isex.getMessage());
            }

            utx.begin();
            Assert.assertEquals(Status.STATUS_ACTIVE, utx.getStatus());

            // rollback transaction.
            utx.rollback();
            Assert.assertEquals(Status.STATUS_NO_TRANSACTION, utx.getStatus());

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

    @Test
    public void testTransactionTimeOut() throws SystemException, NotSupportedException, SecurityException, IllegalStateException, RollbackException, HeuristicMixedException, HeuristicRollbackException
    {
        Assert.assertNotNull(utx);
        Assert.assertSame(KunderaJTAUserTransaction.class, utx.getClass());

        // set timeout before running transaction.
        utx.setTransactionTimeout(20);
        Assert.assertEquals(20, ((KunderaJTAUserTransaction) utx).getTransactionTimeout());

        utx.setTransactionTimeout(0);
        // try after beginning transaction.
        utx.begin();

        utx.setTransactionTimeout(202);
        Assert.assertEquals(0, ((KunderaJTAUserTransaction) utx).getTransactionTimeout());
        
        utx.commit();
    }

    @Test
    public void testMarkedRollback() throws SystemException, NotSupportedException
    {
        Assert.assertNotNull(utx);
        Assert.assertSame(KunderaJTAUserTransaction.class, utx.getClass());

        try
        {
            utx.setRollbackOnly();
            Assert.fail("Should have gone to catch block!");
        }
        catch (IllegalStateException iaex)
        {
            Assert.assertEquals("Cannot get Transaction for setRollbackOnly", iaex.getMessage());
        }

        // try after beginning transaction.
        utx.begin();
        utx.setRollbackOnly();
        Assert.assertEquals(Status.STATUS_MARKED_ROLLBACK, utx.getStatus());
        utx.rollback();
    }

    
    @After
    public void tearDown()
    {
        utx = null;
    }
}
