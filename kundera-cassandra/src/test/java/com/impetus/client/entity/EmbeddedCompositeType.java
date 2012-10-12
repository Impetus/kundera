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
package com.impetus.client.entity;

import javax.persistence.Column;
import javax.persistence.Embeddable;

/**
 * @author vivek.mishra
 *
 */
@Embeddable

public class EmbeddedCompositeType
{

    @Column
    private int key1;
    @Column
    private String key2;
    @Column
    private Double key3;
    /**
     * @return the key1
     */
    public int getKey1()
    {
        return key1;
    }
    /**
     * @param key1 the key1 to set
     */
    public void setKey1(int key1)
    {
        this.key1 = key1;
    }
    /**
     * @return the key2
     */
    public String getKey2()
    {
        return key2;
    }
    /**
     * @param key2 the key2 to set
     */
    public void setKey2(String key2)
    {
        this.key2 = key2;
    }
    /**
     * @return the key3
     */
    public Double getKey3()
    {
        return key3;
    }
    /**
     * @param key3 the key3 to set
     */
    public void setKey3(Double key3)
    {
        this.key3 = key3;
    }
    
    
    
}
