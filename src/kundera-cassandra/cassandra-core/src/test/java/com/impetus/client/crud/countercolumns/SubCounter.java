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
package com.impetus.client.crud.countercolumns;

import javax.persistence.Column;
import javax.persistence.Embeddable;

@Embeddable
public class SubCounter
{

    @Column(name = "SUB_COUNTER")
    private long subCounter;

    @Column(name = "SUB_COUNTER_NAME")
    private transient String subCounter_name;

    /**
     * @return the subCounter
     */
    public long getSubCounter()
    {
        return subCounter;
    }

    /**
     * @param subCounter
     *            the subCounter to set
     */
    public void setSubCounter(long subCounter)
    {
        this.subCounter = subCounter;
    }

    /**
     * @return the subCounter_name
     */
    public String getSubCounter_name()
    {
        return subCounter_name;
    }

    /**
     * @param subCounter_name
     *            the subCounter_name to set
     */
    public void setSubCounter_name(String subCounter_name)
    {
        this.subCounter_name = subCounter_name;
    }

}
