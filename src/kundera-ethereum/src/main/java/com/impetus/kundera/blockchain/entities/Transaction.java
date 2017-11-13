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

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;

/**
 * The Class Transaction.
 * 
 * @author devender.yadav
 */
@Entity
public class Transaction {
	
	/** The hash. */
	@Id
	private String hash;
	
	/** The nonce. */
	@Column
	private String nonce;
	
	/** The block hash. */
	@Column
	private String blockHash;
	
	/** The block number. */
	@Column
	private String blockNumber;
	
	/** The transaction index. */
	@Column
	private String transactionIndex;
	
	/** The from. */
	@Column
	private String from;
	
	/** The to. */
	@Column
	private String to;
	
	/** The value. */
	@Column
	private String value;
	
	/** The gas price. */
	@Column
	private String gasPrice;
	
	/** The gas. */
	@Column
	private String gas;
	
	/** The input. */
	@Column
	private String input;
	
	/** The creates. */
	@Column
	private String creates;
	
	/** The public key. */
	@Column
	private String publicKey;
	
	/** The raw. */
	@Column
	private String raw;
	
	/** The r. */
	@Column
	private String r;
	
	/** The s. */
	@Column
	private String s;
	
	/** The v. */
	@Column
	private int v;

	/**
	 * Gets the hash.
	 *
	 * @return the hash
	 */
	public String getHash() {
		return hash;
	}

	/**
	 * Sets the hash.
	 *
	 * @param hash the new hash
	 */
	public void setHash(String hash) {
		this.hash = hash;
	}

	/**
	 * Gets the nonce.
	 *
	 * @return the nonce
	 */
	public String getNonce() {
		return nonce;
	}

	/**
	 * Sets the nonce.
	 *
	 * @param nonce the new nonce
	 */
	public void setNonce(String nonce) {
		this.nonce = nonce;
	}

	/**
	 * Gets the block hash.
	 *
	 * @return the block hash
	 */
	public String getBlockHash() {
		return blockHash;
	}

	/**
	 * Sets the block hash.
	 *
	 * @param blockHash the new block hash
	 */
	public void setBlockHash(String blockHash) {
		this.blockHash = blockHash;
	}

	/**
	 * Gets the block number.
	 *
	 * @return the block number
	 */
	public String getBlockNumber() {
		return blockNumber;
	}

	/**
	 * Sets the block number.
	 *
	 * @param blockNumber the new block number
	 */
	public void setBlockNumber(String blockNumber) {
		this.blockNumber = blockNumber;
	}

	/**
	 * Gets the transaction index.
	 *
	 * @return the transaction index
	 */
	public String getTransactionIndex() {
		return transactionIndex;
	}

	/**
	 * Sets the transaction index.
	 *
	 * @param transactionIndex the new transaction index
	 */
	public void setTransactionIndex(String transactionIndex) {
		this.transactionIndex = transactionIndex;
	}

	/**
	 * Gets the from.
	 *
	 * @return the from
	 */
	public String getFrom() {
		return from;
	}

	/**
	 * Sets the from.
	 *
	 * @param from the new from
	 */
	public void setFrom(String from) {
		this.from = from;
	}

	/**
	 * Gets the to.
	 *
	 * @return the to
	 */
	public String getTo() {
		return to;
	}

	/**
	 * Sets the to.
	 *
	 * @param to the new to
	 */
	public void setTo(String to) {
		this.to = to;
	}

	/**
	 * Gets the value.
	 *
	 * @return the value
	 */
	public String getValue() {
		return value;
	}

	/**
	 * Sets the value.
	 *
	 * @param value the new value
	 */
	public void setValue(String value) {
		this.value = value;
	}

	/**
	 * Gets the gas price.
	 *
	 * @return the gas price
	 */
	public String getGasPrice() {
		return gasPrice;
	}

	/**
	 * Sets the gas price.
	 *
	 * @param gasPrice the new gas price
	 */
	public void setGasPrice(String gasPrice) {
		this.gasPrice = gasPrice;
	}

	/**
	 * Gets the gas.
	 *
	 * @return the gas
	 */
	public String getGas() {
		return gas;
	}

	/**
	 * Sets the gas.
	 *
	 * @param gas the new gas
	 */
	public void setGas(String gas) {
		this.gas = gas;
	}

	/**
	 * Gets the input.
	 *
	 * @return the input
	 */
	public String getInput() {
		return input;
	}

	/**
	 * Sets the input.
	 *
	 * @param input the new input
	 */
	public void setInput(String input) {
		this.input = input;
	}

	/**
	 * Gets the creates.
	 *
	 * @return the creates
	 */
	public String getCreates() {
		return creates;
	}

	/**
	 * Sets the creates.
	 *
	 * @param creates the new creates
	 */
	public void setCreates(String creates) {
		this.creates = creates;
	}

	/**
	 * Gets the public key.
	 *
	 * @return the public key
	 */
	public String getPublicKey() {
		return publicKey;
	}

	/**
	 * Sets the public key.
	 *
	 * @param publicKey the new public key
	 */
	public void setPublicKey(String publicKey) {
		this.publicKey = publicKey;
	}

	/**
	 * Gets the raw.
	 *
	 * @return the raw
	 */
	public String getRaw() {
		return raw;
	}

	/**
	 * Sets the raw.
	 *
	 * @param raw the new raw
	 */
	public void setRaw(String raw) {
		this.raw = raw;
	}

	/**
	 * Gets the r.
	 *
	 * @return the r
	 */
	public String getR() {
		return r;
	}

	/**
	 * Sets the r.
	 *
	 * @param r the new r
	 */
	public void setR(String r) {
		this.r = r;
	}

	/**
	 * Gets the s.
	 *
	 * @return the s
	 */
	public String getS() {
		return s;
	}

	/**
	 * Sets the s.
	 *
	 * @param s the new s
	 */
	public void setS(String s) {
		this.s = s;
	}

	/**
	 * Gets the v.
	 *
	 * @return the v
	 */
	public int getV() {
		return v;
	}

	/**
	 * Sets the v.
	 *
	 * @param v the new v
	 */
	public void setV(int v) {
		this.v = v;
	}

}