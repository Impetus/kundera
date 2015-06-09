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
 /*
 * author: karthikp.manchala
 */
package com.impetus.client.cassandra.udt;

import java.util.Set;

import javax.persistence.Column;
import javax.persistence.Embeddable;

// TODO: Auto-generated Javadoc
/**
 * The Class Phone.
 */
@Embeddable
public class Phone
{
    
    /** The number. */
    @Column
    private Long number;

     /** The tags. */
     @Column
     private Set<String> tags;

    /**
     * Instantiates a new phone.
     */
    public Phone()
    {

    }

    /**
     * Gets the number.
     *
     * @return the number
     */
    public Long getNumber()
    {
        return number;
    }

    /**
     * Sets the number.
     *
     * @param number the new number
     */
    public void setNumber(Long number)
    {
        this.number = number;
    }

     /**
      * Gets the tags.
      *
      * @return the tags
      */
     public Set<String> getTags()
     {
     return tags;
     }
    
     /**
      * Sets the tags.
      *
      * @param tags the new tags
      */
     public void setTags(Set<String> tags)
     {
     this.tags = tags;
     }

}
