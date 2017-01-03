/*******************************************************************************
* * Copyright 2017 Impetus Infotech.
* *
* * Licensed under the Apache License, Version 2.0 (the "License");
* * you may not use this file except in compliance with the License.
* * You may obtain a copy of the License at
* *
* * http://www.apache.org/licenses/LICENSE-2.0
* *
* * Unless required by applicable law or agreed to in writing, software
* * distributed under the License is distributed on an "AS IS" BASIS,
* * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* * See the License for the specific language governing permissions and
* * limitations under the License.
******************************************************************************/
package com.impetus.kundera.client.crud.entitylisteners;

import javax.persistence.EntityListeners;
import javax.persistence.MappedSuperclass;

/**
 * The Class AbstractSuperClass.
 */
@MappedSuperclass
@EntityListeners(Listener.class)
public abstract class AbstractSuperClass
{

    /** The extern id. */
    private String externId;

    /**
     * Instantiates a new abstract super class.
     */
    public AbstractSuperClass()
    {
    }

    /**
     * Gets the extern id.
     * 
     * @return the extern id
     */
    public String getExternId()
    {
        return this.externId;
    }

    /**
     * Sets the extern id.
     * 
     * @param externId
     *            the new extern id
     */
    public void setExternId(String externId)
    {
        this.externId = externId;
    }
}
