/*******************************************************************************
 * * Copyright 2015 Impetus Infotech.
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
package com.impetus.client.hbase.secondarytable;

import javax.persistence.Column;
import javax.persistence.Embeddable;

/**
 * The Class EmbeddedCollectionEntity.
 * 
 * @author Pragalbh Garg
 */
@Embeddable
public class EmbeddedCollectionEntity
{

    /** The collection id. */
    @Column(name = "COLLECTION_ID")
    private String collectionId;

    /** The collection name. */
    @Column(name = "COLLECTION_NAME")
    private String collectionName;

    /**
     * Gets the collection id.
     * 
     * @return the collection id
     */
    public String getCollectionId()
    {
        return collectionId;
    }

    /**
     * Sets the collection id.
     * 
     * @param collectionId
     *            the new collection id
     */
    public void setCollectionId(String collectionId)
    {
        this.collectionId = collectionId;
    }

    /**
     * Gets the collection name.
     * 
     * @return the collection name
     */
    public String getCollectionName()
    {
        return collectionName;
    }

    /**
     * Sets the collection name.
     * 
     * @param collectionName
     *            the new collection name
     */
    public void setCollectionName(String collectionName)
    {
        this.collectionName = collectionName;
    }

}
