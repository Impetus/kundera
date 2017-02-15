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

import javax.persistence.Embeddable;

/**
 * The Class MetricsId.
 */
@Embeddable
public class MetricsId
{

    /** The host. */
    private String host;

    /** The metric. */
    private String metric;

    /** The time. */
    private long time;

    /**
     * Instantiates a new metrics id.
     */
    public MetricsId()
    {
    }

    /**
     * Instantiates a new metrics id.
     *
     * @param host
     *            the host
     * @param metric
     *            the metric
     * @param time
     *            the time
     */
    public MetricsId(String host, String metric, long time)
    {
        this.host = host;
        this.metric = metric;
        this.time = time;
    }

    /**
     * Gets the host.
     *
     * @return the host
     */
    public String getHost()
    {
        return host;
    }

    /**
     * Sets the host.
     *
     * @param host
     *            the new host
     */
    public void setHost(String host)
    {
        this.host = host;
    }

    /**
     * Gets the metric.
     *
     * @return the metric
     */
    public String getMetric()
    {
        return metric;
    }

    /**
     * Sets the metric.
     *
     * @param metric
     *            the new metric
     */
    public void setMetric(String metric)
    {
        this.metric = metric;
    }

    /**
     * Gets the time.
     *
     * @return the time
     */
    public long getTime()
    {
        return time;
    }

    /**
     * Sets the time.
     *
     * @param time
     *            the new time
     */
    public void setTime(long time)
    {
        this.time = time;
    }

}
