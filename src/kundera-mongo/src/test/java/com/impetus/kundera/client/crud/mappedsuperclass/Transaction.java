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

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.DiscriminatorColumn;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Inheritance;
import javax.persistence.InheritanceType;
import javax.persistence.OneToOne;
import javax.persistence.Table;



/**
 * @author vivek.mishra
 * Transaction concrete entity
 *
 */
@Entity
@Table(name = "TRNX")
@Inheritance(strategy = InheritanceType.SINGLE_TABLE)
@DiscriminatorColumn(name = "tx_type")
public class Transaction extends AbstractTransaction
{
    
    @Column
    private String bankIdentifier;
    
    @OneToOne(cascade = { CascadeType.ALL },fetch = FetchType.EAGER)
    private Ledger ledger;

    public String getBankIdentifier()
    {
        return bankIdentifier;
    }

    public void setBankIdentifier(String bankIdentifier)
    {
        this.bankIdentifier = bankIdentifier;
    }
    
    public Ledger getLedger()
    {
        return ledger;
    }

    public void setLedger(Ledger ledger)
    {
        this.ledger = ledger;
    }

}
