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
package com.impetus.kundera.blockchain.ethereum;

import java.math.BigInteger;

import com.impetus.kundera.blockchain.entities.Block;

/**
 * The Class BlockchainImporter.
 * 
 * @author devender.yadav
 * 
 */
public class BlockchainImporter
{

    /** The client. */
    private static KunderaWeb3jClient client;


    /**
     * Instantiates a new blockchain importer.
     */
    private BlockchainImporter()
    {
    }

    /**
     * Gets the kundera web3j client.
     *
     * @return the kundera web3j client
     */
    public static KunderaWeb3jClient getKunderaWeb3jClient()
    {
        return client;
    }
    
    /**
     * Import all blocks from EARLIEST to LATEST and keep importing upcoming new
     * blocks
     */
    public void importUptoLatestAndSyncNewBlocks()
    {

        client.saveBlocksTillLatest(client.getFirstBlockNumber(), true);
    }

    /**
     * Import all blocks from startBlockNumber to LATEST and keep importing
     * upcoming new blocks
     * 
     * @param startBlockNumber
     *            the start block number
     */
    public void importUptoLatestAndSyncNewBlocks(BigInteger startBlockNumber)
    {
        client.saveBlocksTillLatest(startBlockNumber, true);
    }

    /**
     * Import all blocks from EARLIEST to LATEST
     * 
     */
    public void importUptoLatestBlock()
    {
        client.saveBlocksTillLatest(client.getFirstBlockNumber(), false);
    }

    /**
     * Import all blocks from startBlockNumber to LATEST
     *
     * @param startBlockNumber
     *            the start block number
     */
    public void importUptoLatestBlock(BigInteger startBlockNumber)
    {
        client.saveBlocksTillLatest(startBlockNumber, false);
    }

    /**
     * Import all blocks from EARLIEST to endBlockNumber
     *
     *
     * @param endBlockNumber
     *            the end block number
     */
    public void importBlocksFromStarting(BigInteger endBlockNumber)
    {
        client.saveBlocks(client.getFirstBlockNumber(), endBlockNumber);
    }

    /**
     * Import all blocks from startBlockNumber to endBlockNumber
     *
     * @param startBlockNumber
     *            the start block number
     * @param endBlockNumber
     *            the end block number
     */
    public void importBlocks(BigInteger startBlockNumber, BigInteger endBlockNumber)
    {
        client.saveBlocks(startBlockNumber, endBlockNumber);
    }

    /**
     * Gets the first block.
     *
     * @return the first block
     */
    public Block getFirstBlock()
    {
        return EtherObjectConverterUtil.convertEtherBlockToKunderaBlock(client.getFirstBlock(false), false);
    }

    /**
     * Gets the first block with transactions.
     *
     * @return the first block with transactions
     */
    public Block getFirstBlockWithTransactions()
    {
        return EtherObjectConverterUtil.convertEtherBlockToKunderaBlock(client.getFirstBlock(true));
    }

    /**
     * Gets the first block number.
     *
     * @return the first block number
     */
    public BigInteger getFirstBlockNumber()
    {
        return client.getFirstBlockNumber();
    }

    /**
     * Gets the latest block.
     *
     * @return the latest block
     */
    public Block getLatestBlock()
    {
        return EtherObjectConverterUtil.convertEtherBlockToKunderaBlock(client.getLatestBlock(false), false);
    }

    /**
     * Gets the latest block with transactions.
     *
     * @return the latest block with transactions
     */
    public Block getLatestBlockWithTransactions()
    {
        return EtherObjectConverterUtil.convertEtherBlockToKunderaBlock(client.getLatestBlock(true));
    }

    /**
     * Gets the latest block number.
     *
     * @return the latest block number
     */
    public BigInteger getLatestBlockNumber()
    {
        return client.getLatestBlockNumber();
    }

    /**
     * Gets the block.
     *
     * @param blockNumber
     *            the block number
     * @return the block
     */
    public Block getBlock(BigInteger blockNumber)
    {
        return EtherObjectConverterUtil.convertEtherBlockToKunderaBlock(client.getBlock(blockNumber, false), false);
    }

    /**
     * Gets the block with transactions.
     *
     * @param blockNumber
     *            the block number
     * @return the block with transactions
     */
    public Block getBlockWithTransactions(BigInteger blockNumber)
    {
        return EtherObjectConverterUtil.convertEtherBlockToKunderaBlock(client.getBlock(blockNumber, true));
    }

    /**
     * Initialize Web3j and Kundera related parameters.
     *
     * @return the blockchainImporter object
     */
    public static BlockchainImporter initialize()
    {
        client = new KunderaWeb3jClient();
        return new BlockchainImporter();
    }

    /**
     * Destroy.
     */
    public void destroy()
    {
        client.destroy();
    }

}
