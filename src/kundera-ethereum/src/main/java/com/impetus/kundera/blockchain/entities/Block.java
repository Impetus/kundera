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
package com.impetus.kundera.blockchain.entities;

import java.util.List;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;

/**
 * The Class Block.
 * 
 * @author devender.yadav
 * 
 */
@Entity
public class Block
{

    /** The number. */
    @Id
    private String number;

    /** The hash. */
    @Column
    private String hash;

    /** The parent hash. */
    @Column
    private String parentHash;

    /** The nonce. */
    @Column
    private String nonce;

    /** The sha3 uncles. */
    @Column
    private String sha3Uncles;

    /** The logs bloom. */
    @Column
    private String logsBloom;

    /** The transactions root. */
    @Column
    private String transactionsRoot;

    /** The state root. */
    @Column
    private String stateRoot;

    /** The receipts root. */
    @Column
    private String receiptsRoot;

    /** The author. */
    @Column
    private String author;

    /** The miner. */
    @Column
    private String miner;

    /** The mix hash. */
    @Column
    private String mixHash;

    /** The difficulty. */
    @Column
    private String difficulty;

    /** The total difficulty. */
    @Column
    private String totalDifficulty;

    /** The extra data. */
    @Column
    private String extraData;

    /** The size. */
    @Column
    private String size;

    /** The gas limit. */
    @Column
    private String gasLimit;

    /** The gas used. */
    @Column
    private String gasUsed;

    /** The timestamp. */
    @Column
    private String timestamp;

    /** The transactions. */
    @Column
    private List<Transaction> transactions;

    /** The uncles. */
    @Column
    private List<String> uncles;

    /** The seal fields. */
    @Column
    private List<String> sealFields;

    /**
     * Gets the number.
     *
     * @return the number
     */
    public String getNumber()
    {
        return number;
    }

    /**
     * Sets the number.
     *
     * @param number
     *            the new number
     */
    public void setNumber(String number)
    {
        this.number = number;
    }

    /**
     * Gets the hash.
     *
     * @return the hash
     */
    public String getHash()
    {
        return hash;
    }

    /**
     * Sets the hash.
     *
     * @param hash
     *            the new hash
     */
    public void setHash(String hash)
    {
        this.hash = hash;
    }

    /**
     * Gets the parent hash.
     *
     * @return the parent hash
     */
    public String getParentHash()
    {
        return parentHash;
    }

    /**
     * Sets the parent hash.
     *
     * @param parentHash
     *            the new parent hash
     */
    public void setParentHash(String parentHash)
    {
        this.parentHash = parentHash;
    }

    /**
     * Gets the nonce.
     *
     * @return the nonce
     */
    public String getNonce()
    {
        return nonce;
    }

    /**
     * Sets the nonce.
     *
     * @param nonce
     *            the new nonce
     */
    public void setNonce(String nonce)
    {
        this.nonce = nonce;
    }

    /**
     * Gets the sha3 uncles.
     *
     * @return the sha3 uncles
     */
    public String getSha3Uncles()
    {
        return sha3Uncles;
    }

    /**
     * Sets the sha3 uncles.
     *
     * @param sha3Uncles
     *            the new sha3 uncles
     */
    public void setSha3Uncles(String sha3Uncles)
    {
        this.sha3Uncles = sha3Uncles;
    }

    /**
     * Gets the logs bloom.
     *
     * @return the logs bloom
     */
    public String getLogsBloom()
    {
        return logsBloom;
    }

    /**
     * Sets the logs bloom.
     *
     * @param logsBloom
     *            the new logs bloom
     */
    public void setLogsBloom(String logsBloom)
    {
        this.logsBloom = logsBloom;
    }

    /**
     * Gets the transactions root.
     *
     * @return the transactions root
     */
    public String getTransactionsRoot()
    {
        return transactionsRoot;
    }

    /**
     * Sets the transactions root.
     *
     * @param transactionsRoot
     *            the new transactions root
     */
    public void setTransactionsRoot(String transactionsRoot)
    {
        this.transactionsRoot = transactionsRoot;
    }

    /**
     * Gets the state root.
     *
     * @return the state root
     */
    public String getStateRoot()
    {
        return stateRoot;
    }

    /**
     * Sets the state root.
     *
     * @param stateRoot
     *            the new state root
     */
    public void setStateRoot(String stateRoot)
    {
        this.stateRoot = stateRoot;
    }

    /**
     * Gets the receipts root.
     *
     * @return the receipts root
     */
    public String getReceiptsRoot()
    {
        return receiptsRoot;
    }

    /**
     * Sets the receipts root.
     *
     * @param receiptsRoot
     *            the new receipts root
     */
    public void setReceiptsRoot(String receiptsRoot)
    {
        this.receiptsRoot = receiptsRoot;
    }

    /**
     * Gets the author.
     *
     * @return the author
     */
    public String getAuthor()
    {
        return author;
    }

    /**
     * Sets the author.
     *
     * @param author
     *            the new author
     */
    public void setAuthor(String author)
    {
        this.author = author;
    }

    /**
     * Gets the miner.
     *
     * @return the miner
     */
    public String getMiner()
    {
        return miner;
    }

    /**
     * Sets the miner.
     *
     * @param miner
     *            the new miner
     */
    public void setMiner(String miner)
    {
        this.miner = miner;
    }

    /**
     * Gets the mix hash.
     *
     * @return the mix hash
     */
    public String getMixHash()
    {
        return mixHash;
    }

    /**
     * Sets the mix hash.
     *
     * @param mixHash
     *            the new mix hash
     */
    public void setMixHash(String mixHash)
    {
        this.mixHash = mixHash;
    }

    /**
     * Gets the difficulty.
     *
     * @return the difficulty
     */
    public String getDifficulty()
    {
        return difficulty;
    }

    /**
     * Sets the difficulty.
     *
     * @param difficulty
     *            the new difficulty
     */
    public void setDifficulty(String difficulty)
    {
        this.difficulty = difficulty;
    }

    /**
     * Gets the total difficulty.
     *
     * @return the total difficulty
     */
    public String getTotalDifficulty()
    {
        return totalDifficulty;
    }

    /**
     * Sets the total difficulty.
     *
     * @param totalDifficulty
     *            the new total difficulty
     */
    public void setTotalDifficulty(String totalDifficulty)
    {
        this.totalDifficulty = totalDifficulty;
    }

    /**
     * Gets the extra data.
     *
     * @return the extra data
     */
    public String getExtraData()
    {
        return extraData;
    }

    /**
     * Sets the extra data.
     *
     * @param extraData
     *            the new extra data
     */
    public void setExtraData(String extraData)
    {
        this.extraData = extraData;
    }

    /**
     * Gets the size.
     *
     * @return the size
     */
    public String getSize()
    {
        return size;
    }

    /**
     * Sets the size.
     *
     * @param size
     *            the new size
     */
    public void setSize(String size)
    {
        this.size = size;
    }

    /**
     * Gets the gas limit.
     *
     * @return the gas limit
     */
    public String getGasLimit()
    {
        return gasLimit;
    }

    /**
     * Sets the gas limit.
     *
     * @param gasLimit
     *            the new gas limit
     */
    public void setGasLimit(String gasLimit)
    {
        this.gasLimit = gasLimit;
    }

    /**
     * Gets the gas used.
     *
     * @return the gas used
     */
    public String getGasUsed()
    {
        return gasUsed;
    }

    /**
     * Sets the gas used.
     *
     * @param gasUsed
     *            the new gas used
     */
    public void setGasUsed(String gasUsed)
    {
        this.gasUsed = gasUsed;
    }

    /**
     * Gets the timestamp.
     *
     * @return the timestamp
     */
    public String getTimestamp()
    {
        return timestamp;
    }

    /**
     * Sets the timestamp.
     *
     * @param timestamp
     *            the new timestamp
     */
    public void setTimestamp(String timestamp)
    {
        this.timestamp = timestamp;
    }

    /**
     * Gets the transactions.
     *
     * @return the transactions
     */
    public List<Transaction> getTransactions()
    {
        return transactions;
    }

    /**
     * Sets the transactions.
     *
     * @param transactions
     *            the new transactions
     */
    public void setTransactions(List<Transaction> transactions)
    {
        this.transactions = transactions;
    }

    /**
     * Gets the uncles.
     *
     * @return the uncles
     */
    public List<String> getUncles()
    {
        return uncles;
    }

    /**
     * Sets the uncles.
     *
     * @param uncles
     *            the new uncles
     */
    public void setUncles(List<String> uncles)
    {
        this.uncles = uncles;
    }

    /**
     * Gets the seal fields.
     *
     * @return the seal fields
     */
    public List<String> getSealFields()
    {
        return sealFields;
    }

    /**
     * Sets the seal fields.
     *
     * @param sealFields
     *            the new seal fields
     */
    public void setSealFields(List<String> sealFields)
    {
        this.sealFields = sealFields;
    }

}
