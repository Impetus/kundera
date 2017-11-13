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

import java.util.ArrayList;
import java.util.List;

import org.web3j.protocol.core.methods.response.EthBlock;
import org.web3j.protocol.core.methods.response.EthBlock.TransactionObject;
import org.web3j.protocol.core.methods.response.EthBlock.TransactionResult;

import com.impetus.kundera.blockchain.entities.Block;
import com.impetus.kundera.blockchain.entities.Transaction;

/**
 * The Class EtherObjectConverterUtil.
 * 
 * @author devender.yadav
 */
public class EtherObjectConverterUtil
{

    /**
     * Instantiates a new ether object converter util.
     */
    private EtherObjectConverterUtil()
    {
    }

    /**
     * Convert ether block to kundera block.
     *
     * @param block
     *            the block
     * @return the block
     */
    public static Block convertEtherBlockToKunderaBlock(EthBlock block)
    {
        return convertEtherBlockToKunderaBlock(block, true);
    }

    /**
     * Convert ether block to kundera block.
     *
     * @param block
     *            the block
     * @param includeTransactions
     *            the include transactions
     * @return the block
     */
    public static Block convertEtherBlockToKunderaBlock(EthBlock block, boolean includeTransactions)
    {

        Block kunderaBlk = new Block();
        org.web3j.protocol.core.methods.response.EthBlock.Block blk = block.getBlock();

        kunderaBlk.setAuthor(blk.getAuthor());
        kunderaBlk.setDifficulty(blk.getDifficultyRaw());
        kunderaBlk.setExtraData(blk.getExtraData());
        kunderaBlk.setGasLimit(blk.getGasLimitRaw());
        kunderaBlk.setGasUsed(blk.getGasUsedRaw());
        kunderaBlk.setHash(blk.getHash());
        kunderaBlk.setLogsBloom(blk.getLogsBloom());
        kunderaBlk.setMiner(blk.getMiner());
        kunderaBlk.setMixHash(blk.getMixHash());
        kunderaBlk.setNonce(blk.getNonceRaw());
        kunderaBlk.setNumber(blk.getNumberRaw());
        kunderaBlk.setParentHash(blk.getParentHash());
        kunderaBlk.setReceiptsRoot(blk.getReceiptsRoot());
        kunderaBlk.setSealFields(blk.getSealFields());
        kunderaBlk.setSha3Uncles(blk.getSha3Uncles());
        kunderaBlk.setSize(blk.getSizeRaw());
        kunderaBlk.setStateRoot(blk.getStateRoot());
        kunderaBlk.setTimestamp(blk.getTimestampRaw());
        kunderaBlk.setTotalDifficulty(blk.getTotalDifficultyRaw());
        kunderaBlk.setTransactionsRoot(blk.getTransactionsRoot());
        kunderaBlk.setUncles(blk.getUncles());

        if (includeTransactions)
        {
            List<Transaction> kunderaTxs = new ArrayList<>();
            List<TransactionResult> txResults = block.getBlock().getTransactions();

            if (txResults != null && !txResults.isEmpty())
            {
                for (TransactionResult transactionResult : txResults)
                {
                    kunderaTxs.add(convertEtherTxToKunderaTx(transactionResult));
                }
                kunderaBlk.setTransactions(kunderaTxs);
            }
        }
        return kunderaBlk;

    }

    /**
     * Convert ether tx to kundera tx.
     *
     * @param transactionResult
     *            the transaction result
     * @return the transaction
     */
    public static Transaction convertEtherTxToKunderaTx(TransactionResult transactionResult)
    {

        Transaction kunderaTx = new Transaction();
        TransactionObject txObj = (TransactionObject) transactionResult;

        kunderaTx.setBlockHash(txObj.getBlockHash());
        kunderaTx.setBlockNumber(txObj.getBlockNumberRaw());
        kunderaTx.setCreates(txObj.getCreates());
        kunderaTx.setFrom(txObj.getFrom());
        kunderaTx.setGas(txObj.getGasRaw());
        kunderaTx.setGasPrice(txObj.getGasPriceRaw());
        kunderaTx.setHash(txObj.getHash());
        kunderaTx.setInput(txObj.getInput());
        kunderaTx.setNonce(txObj.getNonceRaw());
        kunderaTx.setPublicKey(txObj.getPublicKey());
        kunderaTx.setR(txObj.getR());
        kunderaTx.setRaw(txObj.getRaw());
        kunderaTx.setS(txObj.getS());
        kunderaTx.setTo(txObj.getTo());
        kunderaTx.setTransactionIndex(txObj.getTransactionIndexRaw());
        kunderaTx.setV(txObj.getV());
        kunderaTx.setValue(txObj.getValueRaw());

        return kunderaTx;

    }

}
