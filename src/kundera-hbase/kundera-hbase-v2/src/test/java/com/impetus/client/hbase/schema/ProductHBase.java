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

package com.impetus.client.hbase.schema;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * The Class ProductHBase.
 * 
 * @author Pragalbh Garg
 */
@Entity
@Table(name = "PRODUCT_HBASE", schema = "HBaseNew@schemaTest")
public class ProductHBase
{

    /** The person id. */
    @Id
    @Column(name = "PRODUCT_ID")
    private String productId;

    /** The person name. */
    @Column(name = "PRODUCT_NAME")
    private String productName;

    /** The age. */
    @Column(name = "DESCRIPTION")
    private String description;

    /** The price. */
    @Column(name = "PRICE")
    private int price;

    /**
     * Gets the product id.
     * 
     * @return the product id
     */
    public String getProductId()
    {
        return productId;
    }

    /**
     * Sets the product id.
     * 
     * @param productId
     *            the new product id
     */
    public void setProductId(String productId)
    {
        this.productId = productId;
    }

    /**
     * Gets the product name.
     * 
     * @return the product name
     */
    public String getProductName()
    {
        return productName;
    }

    /**
     * Sets the product name.
     * 
     * @param productName
     *            the new product name
     */
    public void setProductName(String productName)
    {
        this.productName = productName;
    }

    /**
     * Gets the description.
     * 
     * @return the description
     */
    public String getDescription()
    {
        return description;
    }

    /**
     * Sets the description.
     * 
     * @param description
     *            the new description
     */
    public void setDescription(String description)
    {
        this.description = description;
    }

    /**
     * Gets the price.
     * 
     * @return the price
     */
    public int getPrice()
    {
        return price;
    }

    /**
     * Sets the price.
     * 
     * @param price
     *            the new price
     */
    public void setPrice(int price)
    {
        this.price = price;
    }

}
