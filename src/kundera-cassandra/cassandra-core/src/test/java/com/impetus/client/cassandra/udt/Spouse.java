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

import javax.persistence.Column;
import javax.persistence.Embeddable;
import javax.persistence.Embedded;

/**
 * The Class Spouse.
 */
@Embeddable
public class Spouse
{

    /** The fullname. */
    @Embedded
    private Fullname fullname;

    /** The maiden name. */
    @Column
    private String maidenName;

    /** The age. */
    @Column
    private int age;

    /**
     * Instantiates a new spouse.
     */
    public Spouse()
    {

    }

    /**
     * Gets the fullname.
     *
     * @return the fullname
     */
    public Fullname getFullname()
    {
        return fullname;
    }

    /**
     * Sets the fullname.
     *
     * @param fullname the new fullname
     */
    public void setFullname(Fullname fullname)
    {
        this.fullname = fullname;
    }

    /**
     * Gets the maiden name.
     *
     * @return the maiden name
     */
    public String getMaidenName()
    {
        return maidenName;
    }

    /**
     * Sets the maiden name.
     *
     * @param maidenName the new maiden name
     */
    public void setMaidenName(String maidenName)
    {
        this.maidenName = maidenName;
    }

    /**
     * Gets the age.
     *
     * @return the age
     */
    public int getAge()
    {
        return age;
    }

    /**
     * Sets the age.
     *
     * @param age the new age
     */
    public void setAge(int age)
    {
        this.age = age;
    }
}
