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
package com.impetus.kundera.client.crud.mappedsuperclass;

import java.util.Date;

import javax.persistence.Column;
import javax.persistence.DiscriminatorColumn;
import javax.persistence.Id;
import javax.persistence.Inheritance;
import javax.persistence.InheritanceType;
import javax.persistence.MappedSuperclass;
import javax.persistence.Table;

/**
 * @author vivek.mishra
 * Transaction {@link MappedSuperclass}
 *
 */
@MappedSuperclass
@Table(name = "TRNX")
@Inheritance(strategy = InheritanceType.SINGLE_TABLE)
@DiscriminatorColumn(name = "tx_type")
public class Transaction 
{

    @Id
    private String txId;
    
    @Column
    private String bankIdentifier;

    
    @Column
    private Date transactionDt;

    public Date getTransactionDt()
    {
        return transactionDt;
    }

    public void setTransactionDt(Date transactionDt)
    {
        this.transactionDt = transactionDt;
    }



    public String getTxId()
    {
        return txId;
    }

    public void setTxId(String txId)
    {
        this.txId = txId;
    }

    public String getBankIdentifier()
    {
        return bankIdentifier;
    }

    public void setBankIdentifier(String bankIdentifier)
    {
        this.bankIdentifier = bankIdentifier;
    }


}
