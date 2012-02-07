/*******************************************************************************
 * * Copyright 2011 Impetus Infotech.
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
package com.impetus.client.entity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;


/**
 * Entity class for user's IM details.
 *
 * @author amresh.singh
 */

@Entity
@Table(name = "imDetails", schema = "Blog")
public class IMDetail
{

    /** The im detail id. */
    @Id
    private String imDetailId;

    /** The im type. */
    @Column(name = "im_type")
    private String imType;

    /** The im detail. */
    @Column(name = "im_detail")
    private String imDetail;

    /**
     * Instantiates a new iM detail.
     */
    public IMDetail()
    {

    }

    /**
     * Instantiates a new iM detail.
     *
     * @param id the id
     * @param type the type
     * @param detail the detail
     */
    public IMDetail(String id, String type, String detail)
    {
        this.imDetailId = id;
        this.imType = type;
        this.imDetail = detail;
    }

    /**
     * Gets the im detail id.
     *
     * @return the imDetailId
     */
    public String getImDetailId()
    {
        return imDetailId;
    }

    /**
     * Sets the im detail id.
     *
     * @param imDetailId the imDetailId to set
     */
    public void setImDetailId(String imDetailId)
    {
        this.imDetailId = imDetailId;
    }

    /**
     * Gets the im type.
     *
     * @return the imType
     */
    public String getImType()
    {
        return imType;
    }

    /**
     * Sets the im type.
     *
     * @param imType the imType to set
     */
    public void setImType(String imType)
    {
        this.imType = imType;
    }

    /**
     * Gets the im detail.
     *
     * @return the imDetail
     */
    public String getImDetail()
    {
        return imDetail;
    }

    /**
     * Sets the im detail.
     *
     * @param imDetail the imDetail to set
     */
    public void setImDetail(String imDetail)
    {
        this.imDetail = imDetail;
    }

}
