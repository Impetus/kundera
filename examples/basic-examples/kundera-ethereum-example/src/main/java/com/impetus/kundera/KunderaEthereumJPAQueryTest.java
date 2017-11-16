/*******************************************************************************
 *  * Copyright 2017 Impetus Infotech.
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
package com.impetus.kundera;

import java.math.BigInteger;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.impetus.kundera.blockchain.entities.Transaction;
import com.impetus.kundera.blockchain.ethereum.BlockchainImporter;

/**
 * The Class KunderaEthereumJPAQueryTest.
 */
public class KunderaEthereumJPAQueryTest
{

    /** The importer. */
    private static BlockchainImporter importer;

    /**
     * Sets the up before class.
     *
     * @throws Exception
     *             the exception
     */
    @BeforeClass
    public static void SetUpBeforeClass() throws Exception
    {
        importer = BlockchainImporter.initialize();
        // init data
        importer.importBlocks(BigInteger.valueOf(4545110), BigInteger.valueOf(4545112));

    }

    /**
     * Gets the block test.
     *
     * @return the block test
     */
    @Test
    public void testJPAQuery()
    {
        EntityManager em = BlockchainImporter.getKunderaWeb3jClient().getEntityManager();
        Query query = (Query) em.createQuery("Select t.gas,t.gasPrice from Transaction t "
                + "where t.blockNumber='0x455a56' and t.from='0xea674fdde714fd979de3edf0f56aa9716b898ec8'");
        List<Transaction> results = query.getResultList();
        Assert.assertEquals(6, results.size());
    }

    /**
     * Tear down after class.
     *
     * @throws Exception
     *             the exception
     */
    @AfterClass
    public static void tearDownAfterClass() throws Exception
    {
        importer.destroy();
    }

}
