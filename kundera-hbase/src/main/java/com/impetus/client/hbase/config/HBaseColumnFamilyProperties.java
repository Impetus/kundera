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
package com.impetus.client.hbase.config;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.io.hfile.Compression;

/**
 * Class HBase Column family properties has some column family related attribute
 * 
 * @author kuldeep.mishra
 *
 */
public class HBaseColumnFamilyProperties
{
    /** log instance */
    private static Log log = LogFactory.getLog(HBaseColumnFamilyProperties.class);

    /** ttl, time to live instance */
    private int ttl = HColumnDescriptor.DEFAULT_TTL;

    /** maxVersion, instance for max version */
    private int maxVersion = HColumnDescriptor.DEFAULT_VERSIONS;

    /** minVersion, instance for min version */
    private int minVersion = HColumnDescriptor.DEFAULT_MIN_VERSIONS;

    /** algorithm, instance for Compression.Algorithm */
    private Compression.Algorithm algorithm = Compression.Algorithm.NONE;

    /**
     * @return the ttl
     */
    public int getTtl()
    {
        return ttl;
    }

    /**
     * @param ttl
     *            the ttl to set
     */
    public void setTtl(String ttl)
    {

        try
        {
            if (ttl != null)
                this.ttl = Integer.parseInt(ttl);
        }
        catch (NumberFormatException nfe)
        {
            log.warn("Time to live should be numeric");
        }
    }

    /**
     * @return the maxVersion
     */
    public int getMaxVersion()
    {
        return maxVersion;
    }

    /**
     * @param maxVersion
     *            the maxVersion to set
     */
    public void setMaxVersion(String maxVersion)
    {
        try
        {
            if (maxVersion != null)
                this.maxVersion = Integer.parseInt(maxVersion);
        }
        catch (NumberFormatException nfe)
        {
            log.warn("max version should be numeric");
        }
    }

    /**
     * @return the minVersion
     */
    public int getMinVersion()
    {
        return minVersion;
    }

    /**
     * @param minVersion
     *            the minVersion to set
     */
    public void setMinVersion(String minVersion)
    {
        try
        {
            if (minVersion != null)
                this.minVersion = Integer.parseInt(minVersion);
        }
        catch (NumberFormatException nfe)
        {
            log.warn("min version should be numeric");
        }
    }

    /**
     * @return the algorithm
     */
    public Compression.Algorithm getAlgorithm()
    {
        return algorithm;
    }

    /**
     * @param algorithm
     *            the algorithm to set
     */
    public void setAlgorithm(Compression.Algorithm algorithm)
    {
        if (algorithm != null)
            this.algorithm = algorithm;
    }
}
