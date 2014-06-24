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
package com.impetus.client.crud.compositeType;

import java.io.Serializable;

import javax.persistence.Column;
import javax.persistence.Embeddable;

/**
 * 
 * @author Kuldeep.Mishra
 * 
 */
@Embeddable
public class PartitionKey implements Serializable
{

    private static final long serialVersionUID = 100000L;

    @Column
    private String partitionKey1;

    @Column
    private int partitionKey2;

    /**
     * @return the partitionKey1
     */
    public String getPartitionKey1()
    {
        return partitionKey1;
    }

    /**
     * @param partitionKey1
     *            the partitionKey1 to set
     */
    public void setPartitionKey1(String partitionKey1)
    {
        this.partitionKey1 = partitionKey1;
    }

    /**
     * @return the partitionKey2
     */
    public int getPartitionKey2()
    {
        return partitionKey2;
    }

    /**
     * @param partitionKey2
     *            the partitionKey2 to set
     */
    public void setPartitionKey2(int partitionKey2)
    {
        this.partitionKey2 = partitionKey2;
    }

}
