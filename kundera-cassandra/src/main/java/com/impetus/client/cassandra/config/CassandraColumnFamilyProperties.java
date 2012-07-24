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
package com.impetus.client.cassandra.config;

import org.apache.cassandra.db.marshal.BytesType;

/**
 * Class Cassandra Column family properties has some column family related
 * attribute
 * 
 * @author kuldeep.mishra
 * 
 */
public class CassandraColumnFamilyProperties
{
    /** dafault validation class for column family */
    private String default_validation_class = BytesType.class.getSimpleName();

    /** comparator type for column family */
    private String comparator = BytesType.class.getSimpleName();

    /**
     * @return the default_validation_class
     */
    public String getDefault_validation_class()
    {
        return default_validation_class;
    }

    /**
     * @param default_validation_class
     *            the default_validation_class to set
     */
    public void setDefault_validation_class(String default_validation_class)
    {
        this.default_validation_class = default_validation_class;
    }

    /**
     * @return the comparator
     */
    public String getComparator()
    {
        return comparator;
    }

    /**
     * @param comparator
     *            the comparator to set
     */
    public void setComparator(String comparator)
    {
        this.comparator = comparator;
    }
}
