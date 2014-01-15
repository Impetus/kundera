/*******************************************************************************
 * * Copyright 2012 Impetus Infotech.
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
package com.impetus.kundera.metadata.entities;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;

/**
 * @author vivek.mishra
 * 
 */
@Entity
public class MapTypeAssociationEntity
{

    @Id
    private String mapKey;

    @Column
    private byte[] bytes;

    /**
     * @return the setKey
     */
    public String getSetKey()
    {
        return mapKey;
    }

    /**
     * @param setKey
     *            the setKey to set
     */
    public void setSetKey(String setKey)
    {
        this.mapKey = setKey;
    }

    /**
     * @return the bytes
     */
    public byte[] getBytes()
    {
        return bytes;
    }

    /**
     * @param bytes
     *            the bytes to set
     */
    public void setBytes(byte[] bytes)
    {
        this.bytes = bytes;
    }

}
