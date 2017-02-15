/*******************************************************************************
 *  * Copyright 2017 Impetus Infotech.
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

package com.impetus.client.kudu.embeddables;

import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.Table;

/**
 * The Class Metrics.
 */
@Entity
@Table(name = "METRICS", schema = "kudutest@kudu")
public class Metrics
{

    /** The id. */
    @EmbeddedId
    private MetricsId id;

    /** The value. */
    private double value;

    /**
     * Gets the id.
     *
     * @return the id
     */
    public MetricsId getId()
    {
        return id;
    }

    /**
     * Sets the id.
     *
     * @param id
     *            the new id
     */
    public void setId(MetricsId id)
    {
        this.id = id;
    }

    /**
     * Gets the value.
     *
     * @return the value
     */
    public double getValue()
    {
        return value;
    }

    /**
     * Sets the value.
     *
     * @param value
     *            the new value
     */
    public void setValue(double value)
    {
        this.value = value;
    }

}
