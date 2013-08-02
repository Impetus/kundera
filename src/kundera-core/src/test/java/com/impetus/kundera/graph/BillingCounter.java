/**
 * Copyright 2012 Impetus Infotech.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.impetus.kundera.graph;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * Dummy BillingCounter enity class
 * 
 * @author amresh.singh
 */

@Entity
@Table(name = "BILLING_COUNTER", schema = "KunderaTest@kunderatest")
public class BillingCounter
{
    @Id
    @Column(name = "COUNTER_ID")
    private int counterId;

    @Column(name = "COUNTER_CODE")
    private String counterCode;

    public BillingCounter()
    {

    }

    public BillingCounter(int id, String code)
    {
        this.counterId = id;
        this.counterCode = code;
    }

    /**
     * @return the counterId
     */
    public int getCounterId()
    {
        return counterId;
    }

    /**
     * @param counterId
     *            the counterId to set
     */
    public void setCounterId(int counterId)
    {
        this.counterId = counterId;
    }

    /**
     * @return the counterCode
     */
    public String getCounterCode()
    {
        return counterCode;
    }

    /**
     * @param counterCode
     *            the counterCode to set
     */
    public void setCounterCode(String counterCode)
    {
        this.counterCode = counterCode;
    }

}
