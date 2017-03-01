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
package com.impetus.client.kudu.entities;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import com.impetus.kundera.index.Index;
import com.impetus.kundera.index.IndexCollection;

/**
 * The Class Item.
 * 
 * @author: devender.yadav
 */
@Entity
@Table(name = "ITEM_KUDU", schema = "kudutest@esIndexerTest")
@IndexCollection(columns = { @Index(name = "name"), @Index(name = "quantity"), @Index(name = "price") })
public class Item
{

    /** The id. */
    @Id
    @Column(name = "ID")
    private String id;

    /** The name. */
    @Column(name = "NAME")
    private String name;

    /** The age. */
    @Column(name = "QUANTITY")
    private int quantity;

    /** The salary. */
    @Column(name = "PRICE")
    private Double price;

    /**
     * Instantiates a new item.
     */
    public Item()
    {
    }

    /**
     * Instantiates a new item.
     *
     * @param id
     *            the id
     * @param name
     *            the name
     * @param quantity
     *            the quantity
     * @param price
     *            the price
     */
    public Item(String id, String name, int quantity, Double price)
    {
        super();
        this.id = id;
        this.name = name;
        this.quantity = quantity;
        this.price = price;
    }

    /**
     * Gets the id.
     *
     * @return the id
     */
    public String getId()
    {
        return id;
    }

    /**
     * Sets the id.
     *
     * @param id
     *            the new id
     */
    public void setId(String id)
    {
        this.id = id;
    }

    /**
     * Gets the name.
     *
     * @return the name
     */
    public String getName()
    {
        return name;
    }

    /**
     * Sets the name.
     *
     * @param name
     *            the new name
     */
    public void setName(String name)
    {
        this.name = name;
    }

    /**
     * Gets the quantity.
     *
     * @return the quantity
     */
    public int getQuantity()
    {
        return quantity;
    }

    /**
     * Sets the quantity.
     *
     * @param quantity
     *            the new quantity
     */
    public void setQuantity(int quantity)
    {
        this.quantity = quantity;
    }

    /**
     * Gets the price.
     *
     * @return the price
     */
    public Double getPrice()
    {
        return price;
    }

    /**
     * Sets the price.
     *
     * @param price
     *            the new price
     */
    public void setPrice(Double price)
    {
        this.price = price;
    }

}
