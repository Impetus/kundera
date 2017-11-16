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

import java.io.IOException;
import java.math.BigInteger;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.core.DefaultBlockParameter;
import org.web3j.protocol.core.DefaultBlockParameterName;
import org.web3j.protocol.core.methods.response.EthBlock;
import org.web3j.protocol.core.methods.response.EthBlock.TransactionResult;
import org.web3j.protocol.http.HttpService;
import org.web3j.protocol.ipc.UnixIpcService;
import org.web3j.protocol.ipc.WindowsIpcService;
import org.web3j.utils.Numeric;

import com.impetus.kundera.KunderaException;
import com.impetus.kundera.blockchain.entities.Block;
import com.impetus.kundera.blockchain.entities.Transaction;
import com.impetus.kundera.blockchain.util.EthConstants;
import com.impetus.kundera.blockchain.util.KunderaPropertyBuilder;
import com.impetus.kundera.blockchain.util.PropertyReader;
import com.impetus.kundera.client.ClientResolverException;

/**
 * The Class KunderaWeb3jClient.
 * 
 * @author devender.yadav
 */
public class KunderaWeb3jClient
{

    /** The Constant LOGGER. */
    private static final Logger LOGGER = LoggerFactory.getLogger(KunderaWeb3jClient.class);

    /** The web3j. */
    private Web3j web3j;

    /** The emf. */
    private EntityManagerFactory emf;

    /** The em. */
    private EntityManager em;

    /** The reader. */
    private PropertyReader reader;

    /**
     * Instantiates a new kundera web3j client.
     */
    public KunderaWeb3jClient()
    {
        reader = initializeProperties();
        initializeWeb3j(reader);
        initializeKunderaParams(reader);
    }

    /**
     * Fetch blocks till latest.
     *
     * @param startBlockNum
     *            the start block number
     * @param syncUpcomingblocks
     *            the sync upcomingblocks
     */
    public void saveBlocksTillLatest(BigInteger startBlockNum, boolean syncUpcomingblocks)
    {

        if (syncUpcomingblocks)
        {
            web3j.catchUpToLatestAndSubscribeToNewBlocksObservable(DefaultBlockParameter.valueOf(startBlockNum), true)
                    .subscribe(block -> {
                        persistBlocksAndTransactions(block);
                    });
        }
        else
        {
            web3j.catchUpToLatestBlockObservable(DefaultBlockParameter.valueOf(startBlockNum), true).subscribe(
                    block -> {
                        persistBlocksAndTransactions(block);
                    });
        }
    }

    /**
     * Persist blocks and transactions.
     *
     * @param block
     *            the block
     */
    private void persistBlocksAndTransactions(EthBlock block)
    {
        Block blk = EtherObjectConverterUtil.convertEtherBlockToKunderaBlock(block, false);
        try
        {
            em.persist(blk);
            LOGGER.debug("Block number " + getBlockNumberWithRawData(blk.getNumber()) + " is stored!");
        }
        catch (Exception ex)
        {
            LOGGER.error("Block number " + getBlockNumberWithRawData(blk.getNumber()) + " is not stored. ", ex);
            throw new KunderaException("Block number " + getBlockNumberWithRawData(blk.getNumber())
                    + " is not stored. ", ex);
        }
        for (TransactionResult tx : block.getResult().getTransactions())
        {
            persistTransactions(EtherObjectConverterUtil.convertEtherTxToKunderaTx(tx), blk.getNumber());
        }
        em.clear();
    }

    /**
     * Persist transactions.
     *
     * @param tx
     *            the transaction
     * @param blockNumber
     *            the block number
     */
    private void persistTransactions(Transaction tx, String blockNumber)
    {

        LOGGER.debug("Going to save transactions for Block - " + getBlockNumberWithRawData(blockNumber) + "!");
        try
        {
            em.persist(tx);
            LOGGER.debug("Transaction with hash " + tx.getHash() + " is stored!");
        }
        catch (Exception ex)
        {
            LOGGER.error("Transaction with hash " + tx.getHash() + " is not stored. ", ex);
            throw new KunderaException("transaction with hash " + tx.getHash() + " is not stored. ", ex);
        }

    }

    /**
     * Gets the block.
     *
     * @param number
     *            the number
     * @param includeTransactions
     *            the include transactions
     * @return the block
     */
    public EthBlock getBlock(BigInteger number, boolean includeTransactions)
    {
        try
        {
            return web3j.ethGetBlockByNumber(DefaultBlockParameter.valueOf(number), includeTransactions).send();
        }
        catch (IOException ex)
        {
            LOGGER.error("Not able to find block" + number + ". ", ex);
            throw new KunderaException("Not able to find block" + number + ". ", ex);
        }
    }

    /**
     * Gets the latest block.
     *
     * @param includeTransactions
     *            the include transactions
     * @return the latest block
     */
    public EthBlock getLatestBlock(boolean includeTransactions)
    {
        try
        {
            return web3j.ethGetBlockByNumber(DefaultBlockParameterName.LATEST, includeTransactions).send();
        }
        catch (IOException ex)
        {
            LOGGER.error("Not able to find the LATEST block. ", ex);
            throw new KunderaException("Not able to find the LATEST block. ", ex);
        }
    }

    /**
     * Gets the latest block number.
     *
     * @return the latest block number
     */
    public BigInteger getLatestBlockNumber()
    {
        return getLatestBlock(false).getBlock().getNumber();
    }

    /**
     * Gets the first block.
     *
     * @param includeTransactions
     *            the include transactions
     * @return the first block
     */
    public EthBlock getFirstBlock(boolean includeTransactions)
    {
        try
        {
            return web3j.ethGetBlockByNumber(DefaultBlockParameterName.EARLIEST, includeTransactions).send();
        }
        catch (IOException ex)
        {
            LOGGER.error("Not able to find the EARLIEST block. ", ex);
            throw new KunderaException("Not able to find the EARLIEST block. ", ex);
        }

    }

    /**
     * Gets the first block number.
     *
     * @return the first block number
     */
    public BigInteger getFirstBlockNumber()
    {
        return getFirstBlock(false).getBlock().getNumber();
    }

    /**
     * Gets the blocks.
     *
     * @param startBlockNumber
     *            the start block number
     * @param endBlockNumber
     *            the end block number
     * @return the blocks
     */
    public void saveBlocks(BigInteger startBlockNumber, BigInteger endBlockNumber)
    {

        if (endBlockNumber.compareTo(startBlockNumber) < 1)
        {
            LOGGER.error("startBlockNumber must be smaller than endBlockNumer");
            throw new KunderaException("startBlockNumber must be smaller than endBlockNumer");
        }

        if (startBlockNumber.compareTo(getFirstBlockNumber()) < 0)
        {
            LOGGER.error("start block number can't be smaller than EARLIEST block");
            throw new KunderaException("starting block number can't be smaller than EARLIEST block");
        }

        if (endBlockNumber.compareTo(getLatestBlockNumber()) > 0)
        {
            LOGGER.error("end block number can't be larger than LATEST block");
            throw new KunderaException("end block number can't be larger than LATEST block");
        }

        long diff = endBlockNumber.subtract(startBlockNumber).longValue();

        for (long i = 0; i <= diff; i++)
        {
            EthBlock block = null;
            BigInteger currentBlockNum = null;
            try
            {
                currentBlockNum = startBlockNumber.add(BigInteger.valueOf(i));
                block = web3j.ethGetBlockByNumber(DefaultBlockParameter.valueOf(currentBlockNum), true).send();
            }
            catch (IOException ex)
            {
                LOGGER.error("Not able to find block" + currentBlockNum + ". ", ex);
                throw new KunderaException("Not able to find block" + currentBlockNum + ". ", ex);
            }
            persistBlocksAndTransactions(block);
        }

    }

    /**
     * Initialize web3j.
     *
     * @param reader
     *            the reader
     */
    private void initializeWeb3j(PropertyReader reader)
    {

        String endPoint = reader.getProperty(EthConstants.ETHEREUM_NODE_ENDPOINT);

        if (endPoint == null || endPoint.isEmpty())
        {
            LOGGER.error("Specify - " + EthConstants.ETHEREUM_NODE_ENDPOINT + " in "
                    + EthConstants.KUNDERA_ETHEREUM_PROPERTIES);
            throw new KunderaException("Specify - " + EthConstants.ETHEREUM_NODE_ENDPOINT + " in "
                    + EthConstants.KUNDERA_ETHEREUM_PROPERTIES);
        }

        else if (endPoint.contains(".ipc"))
        {
            String os = reader.getProperty(EthConstants.ETHEREUM_NODE_OS);
            LOGGER.info("Connecting using IPC socket. IPC Socket File location - " + endPoint);
            if (os != null && "windows".equalsIgnoreCase(os))
            {
                web3j = Web3j.build(new WindowsIpcService(endPoint));
            }
            else
            {
                web3j = Web3j.build(new UnixIpcService(endPoint));
            }
        }

        else
        {
            LOGGER.info("Connecting via Endpoint - " + endPoint);
            web3j = Web3j.build(new HttpService(endPoint));
        }
    }

    /**
     * Initialize kundera params.
     *
     * @param reader
     *            the reader
     */
    private void initializeKunderaParams(PropertyReader reader)
    {
        Map<String, String> props = KunderaPropertyBuilder.populatePersistenceUnitProperties(reader);
        try
        {
            emf = Persistence.createEntityManagerFactory(EthConstants.PU, props);
        }
        catch (ClientResolverException ex)
        {
            LOGGER.error("Not able to find dependency for Kundera " + props.get(EthConstants.KUNDERA_DIALECT)
                    + " client.", ex);
            throw new KunderaException("Not able to find dependency for Kundera "
                    + props.get(EthConstants.KUNDERA_DIALECT) + " client.", ex);
        }
        LOGGER.info("Kundera EMF created...");
        em = emf.createEntityManager();
        LOGGER.info("Kundera EM created...");
    }

    /**
     * Initialize properties.
     *
     * @return the property reader
     */
    private PropertyReader initializeProperties()
    {
        return new PropertyReader(EthConstants.KUNDERA_ETHEREUM_PROPERTIES);
    }

    /**
     * Gets the block number with raw data.
     * 
     * It is used for logging purpose
     *
     * @param blockNumberRaw
     *            the block number raw
     * @return the block number with raw data
     */
    private String getBlockNumberWithRawData(String blockNumberRaw)
    {
        return Numeric.decodeQuantity(blockNumberRaw) + "(" + blockNumberRaw + ")";
    }

    /**
     * Gets the EntityManager.
     *
     * @return the EntityManager
     */
    public EntityManager getEntityManager()
    {
        return emf.createEntityManager();
    }

    /**
     * Destroy.
     */
    public void destroy()
    {
        if (em != null)
        {
            em.close();
            LOGGER.info("Kundera EM closed...");
        }

        if (emf != null)
        {
            emf.close();
            LOGGER.info("Kundera EMF closed...");
        }
    }

}
