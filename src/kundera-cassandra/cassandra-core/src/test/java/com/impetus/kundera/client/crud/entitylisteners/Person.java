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

import javax.persistence.Entity;
import javax.persistence.Id;

/**
 * The Class Person.
 */
@Entity
public class Person extends AbstractSuperClass
{

    /** The id. */
    @Id
    private int id;

    /**
     * Gets the id.
     * 
     * @return the id
     */
    public int getId()
    {
        return id;
    }

    /**
     * Sets the id.
     * 
     * @param id
     *            the new id
     */
    public void setId(int id)
    {
        this.id = id;
    }

}