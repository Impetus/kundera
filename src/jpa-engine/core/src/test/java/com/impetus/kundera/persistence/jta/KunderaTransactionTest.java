/*******************************************************************************
 * * Copyright 2013 Impetus Infotech.
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
 
import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.SystemException;

import junit.framework.Assert;

import org.junit.Test;

import com.impetus.kundera.persistence.ResourceManager;

/**
 * @author vivek.mishra
 * junit for {@link KunderaTransaction}
 *
 */
public class KunderaTransactionTest
{

    @Test
    public void test() throws SecurityException, IllegalStateException, RollbackException, HeuristicMixedException, HeuristicRollbackException, SystemException
    {
        KunderaTransaction tx = new KunderaTransaction(1000); //create new instance.
        tx.setImplementor(new DummyResourceImplementor()); // add resource implementor
        Assert.assertEquals(Status.STATUS_ACTIVE, tx.getStatus());
        tx.commit(); // on commit                                      
        Assert.assertEquals(Status.STATUS_COMMITTED, tx.getStatus());
        tx.setRollbackOnly(); // set rollback only
        Assert.assertEquals(Status.STATUS_MARKED_ROLLBACK, tx.getStatus());
        
        tx.commit();
        Assert.assertNotSame(Status.STATUS_COMMITTED, tx.getStatus());
        Assert.assertFalse(tx.delistResource(null, 0));  // no implementation, will always return false.
        Assert.assertFalse(tx.enlistResource(null));     // no implementation, will always return false.
        Assert.assertEquals(1000,tx.getTransactionTimeout());  // get transaction time out
        
        tx.rollback();
        Assert.assertEquals(Status.STATUS_ROLLEDBACK, tx.getStatus());
        
        try
        {
            tx.registerSynchronization(null);
            Assert.fail("Should have gone to catch block!");
        }catch(UnsupportedOperationException uoex)
        {
            Assert.assertNotNull(uoex.getMessage());
        }
        
        
        
    }

    
    private class DummyResourceImplementor implements ResourceManager
    {

        @Override
        public void doCommit()
        {
            // Do nothing.
            
        }

        @Override
        public void doRollback()
        {
            // Do nothing.
            
        }
        
    }
}
