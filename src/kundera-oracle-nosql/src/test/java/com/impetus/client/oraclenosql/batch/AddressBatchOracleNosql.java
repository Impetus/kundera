/**
 * Copyright 2012 Impetus Infotech.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.impetus.client.oraclenosql.batch;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import com.impetus.kundera.index.Index;
import com.impetus.kundera.index.IndexCollection;

/**
 * Entity class for batch operation
 * 
 * @author amresh.singh
 */

@Entity
@Table(name = "ADDRESS_BATCH", schema = "OracleNoSqlTests@oracleNosqlBatchTestSizeTwenty")
@IndexCollection(columns = { @Index(name = "STREET") })
public class AddressBatchOracleNosql
{
    @Id
    @Column(name = "ADDRESS_ID")
    private String addressId;

    /** The person name. */
    @Column(name = "STREET")
    private String street;

    /**
     * @return the addressId
     */
    public String getAddressId()
    {
        return addressId;
    }

    /**
     * @param addressId
     *            the addressId to set
     */
    public void setAddressId(String addressId)
    {
        this.addressId = addressId;
    }

    /**
     * @return the street
     */
    public String getStreet()
    {
        return street;
    }

    /**
     * @param street
     *            the street to set
     */
    public void setStreet(String street)
    {
        this.street = street;
    }
}
