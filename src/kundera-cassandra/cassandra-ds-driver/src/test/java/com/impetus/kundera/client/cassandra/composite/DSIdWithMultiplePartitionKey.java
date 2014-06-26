/*******************************************************************************
 * * Copyright 2014 Impetus Infotech.
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
package com.impetus.kundera.client.cassandra.composite;

import java.io.Serializable;

import javax.persistence.Column;
import javax.persistence.Embeddable;
import javax.persistence.Embedded;

/**
 * 
 * @author Kuldeep.Mishra
 * 
 */
@Embeddable
public class DSIdWithMultiplePartitionKey implements Serializable
{
    private static final long serialVersionUID = 10000000L;

    @Embedded
    private DSPartitionKey partitionKey;

    @Column
    private String clusterkey1;

    @Column
    private int clusterkey2;

    /**
     * @return the partitionKey
     */
    public DSPartitionKey getPartitionKey()
    {
        return partitionKey;
    }

    /**
     * @param partitionKey
     *            the partitionKey to set
     */
    public void setPartitionKey(DSPartitionKey partitionKey)
    {
        this.partitionKey = partitionKey;
    }

    /**
     * @return the clusterkey1
     */
    public String getClusterkey1()
    {
        return clusterkey1;
    }

    /**
     * @param clusterkey1
     *            the clusterkey1 to set
     */
    public void setClusterkey1(String clusterkey1)
    {
        this.clusterkey1 = clusterkey1;
    }

    /**
     * @return the clusterkey2
     */
    public int getClusterkey2()
    {
        return clusterkey2;
    }

    /**
     * @param clusterkey2
     *            the clusterkey2 to set
     */
    public void setClusterkey2(int clusterkey2)
    {
        this.clusterkey2 = clusterkey2;
    }

}
