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

import javax.persistence.AttributeOverride;
import javax.persistence.AttributeOverrides;
import javax.persistence.Column;
import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;

/**
 * @author vivek.mishra
 * Debit transaction extends {@link Transaction}
 *
 */
@Entity
@DiscriminatorValue(value="DEBIT")

@AttributeOverrides(value = { @AttributeOverride(name = "txId", column = @Column(name = "DEBIT_ID")),
        @AttributeOverride(name = "bankIdentifier", column = @Column(name = "DEBIT_BANK_IDENT")),
        @AttributeOverride(name = "transactionDt", column = @Column(name = "TX_DT")) })

public class DebitTransaction extends Transaction
{

    @Column
    private Integer amount;

    @Column
    @Enumerated(EnumType.STRING)
    private Status txStatus;

    public DebitTransaction()
    {
        
    }
    
    public Integer getAmount()
    {
        return amount;
    }

    public void setAmount(Integer amount)
    {
        this.amount = amount;
    }

    public Status getTxStatus()
    {
        return txStatus;
    }

    public void setTxStatus(Status txStatus)
    {
        this.txStatus = txStatus;
    }

}
