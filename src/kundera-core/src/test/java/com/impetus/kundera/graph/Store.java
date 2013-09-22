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

import java.util.ArrayList;
import java.util.List;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.OneToMany;
import javax.persistence.Table;

/**
 * Dummy Store entity class
 * 
 * @author amresh.singh
 */

@Entity
@Table(name = "STORE", schema = "KunderaTest@kunderatest")
public class Store
{
    @Id
    @Column(name = "STORE_ID")
    private int storeId;

    @Column(name = "STORE_NAME")
    private String storeName;

    @OneToMany(cascade = CascadeType.ALL, fetch = FetchType.LAZY)
    @JoinColumn(name = "STORE_ID")
    private List<BillingCounter> counters;

    public Store()
    {

    }

    public Store(int id, String name)
    {
        this.storeId = id;
        this.storeName = name;
    }

    /**
     * @return the storeId
     */
    public int getStoreId()
    {
        return storeId;
    }

    /**
     * @param storeId
     *            the storeId to set
     */
    public void setStoreId(int storeId)
    {
        this.storeId = storeId;
    }

    /**
     * @return the storeName
     */
    public String getStoreName()
    {
        return storeName;
    }

    /**
     * @param storeName
     *            the storeName to set
     */
    public void setStoreName(String storeName)
    {
        this.storeName = storeName;
    }

    /**
     * @return the counters
     */
    public List<BillingCounter> getCounters()
    {
        return counters;
    }

    /**
     * @param counters
     *            the counters to set
     */
    public void addCounter(BillingCounter counter)
    {
        if (counters == null)
        {
            counters = new ArrayList<BillingCounter>();
        }
        counters.add(counter);
    }

}
