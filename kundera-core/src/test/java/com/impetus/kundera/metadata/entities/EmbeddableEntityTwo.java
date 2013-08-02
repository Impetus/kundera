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
import javax.persistence.Embeddable;

import com.impetus.kundera.index.Index;
import com.impetus.kundera.index.IndexCollection;

/**
 * Entity with singular attributes for meta model processing.
 * 
 * @author vivek.mishra
 * 
 */

@Embeddable
@IndexCollection(columns={@Index(name="field")})
public class EmbeddableEntityTwo
{
    @Column(name = "field")
    private Float embeddedField;

    @Column(name = "name")
    private String embeddedName;

    /**
     * @return the field
     */
    public Float getField()
    {
        return embeddedField;
    }

    /**
     * @param field
     *            the field to set
     */
    public void setField(Float field)
    {
        this.embeddedField = field;
    }

    /**
     * @return the name
     */
    public String getName()
    {
        return embeddedName;
    }

    /**
     * @param name
     *            the name to set
     */
    public void setName(String name)
    {
        this.embeddedName = name;
    }

}
