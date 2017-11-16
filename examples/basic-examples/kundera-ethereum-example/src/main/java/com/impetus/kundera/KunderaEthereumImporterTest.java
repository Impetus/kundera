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

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.impetus.kundera.blockchain.entities.Block;
import com.impetus.kundera.blockchain.ethereum.BlockchainImporter;

/**
 * The Class KunderaEthereumImporterTest.
 */
public class KunderaEthereumImporterTest
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
    }

    /**
     * Gets the block test.
     *
     * @return the block test
     */
    @Test
    public void getBlockTest()
    {
        Block blk = importer.getBlock(BigInteger.valueOf(1000000));
        Assert.assertNotNull(blk);
        Assert.assertNull(blk.getTransactions());
    }

    /**
     * Gets the block with transactions test.
     *
     * @return the block with transactions test
     */
    @Test
    public void getBlockWithTransactionsTest()
    {
        Block blk = importer.getBlockWithTransactions(BigInteger.valueOf(1000000));
        Assert.assertNotNull(blk);
        Assert.assertNotNull(blk.getTransactions());
    }

    /**
     * Gets the first block test.
     *
     * @return the first block test
     */
    @Test
    public void getFirstBlockTest()
    {
        Block blk = importer.getFirstBlock();
        Assert.assertNotNull(blk);
        Assert.assertNull(blk.getTransactions());
    }

    /**
     * Gets the first block with transactions test.
     *
     * @return the first block with transactions test
     */
    @Test
    public void getFirstBlockWithTransactionsTest()
    {
        Block blk = importer.getFirstBlockWithTransactions();
        Assert.assertNotNull(blk);
        // No transactions in 1st block
        Assert.assertNull(blk.getTransactions());
    }

    /**
     * Gets the first block number test.
     *
     * @return the first block number test
     */
    @Test
    public void getFirstBlockNumberTest()
    {
        BigInteger blkNum = importer.getFirstBlockNumber();
        Assert.assertEquals(blkNum, new BigInteger("0"));
    }

    /**
     * Gets the latest block test.
     *
     * @return the latest block test
     */
    @Test
    public void getLatestBlockTest()
    {
        Block blk = importer.getLatestBlock();
        Assert.assertNotNull(blk);
        Assert.assertNull(blk.getTransactions());
    }

    /**
     * Gets the latest block with transactions test.
     *
     * @return the latest block with transactions test
     */
    @Test
    public void getLatestBlockWithTransactionsTest()
    {
        Block blk = importer.getLatestBlockWithTransactions();
        Assert.assertNotNull(blk);
        Assert.assertNotNull(blk.getTransactions());
    }

    /**
     * Gets the latest block number test.
     *
     * @return the latest block number test
     */
    @Test
    public void getLatestBlockNumberTest()
    {
        BigInteger blkNum = importer.getLatestBlockNumber();
        Assert.assertTrue(blkNum.compareTo(new BigInteger("4000000")) > 0);
    }

    /**
     * Import blocks test.
     */
    @Test
    public void importBlocksTest()
    {
        importer.importBlocks(BigInteger.valueOf(1000000), BigInteger.valueOf(1000010));
    }

    /**
     * Import blocks from starting test.
     */
    @Test
    public void importBlocksFromStartingTest()
    {
        importer.importBlocksFromStarting(BigInteger.valueOf(10));
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
