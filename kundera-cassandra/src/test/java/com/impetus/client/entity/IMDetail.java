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
package com.impetus.kundera.entity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * Entity class for user's IM details
 *
 * @author amresh.singh
 */

@Entity
@Table(name = "imDetails", schema = "Blog")
public class IMDetail
{

    @Id
    private String imDetailId;

    @Column(name = "im_type")
    private String imType;

    @Column(name = "im_detail")
    private String imDetail;

    public IMDetail()
    {

    }

    public IMDetail(String id, String type, String detail)
    {
        this.imDetailId = id;
        this.imType = type;
        this.imDetail = detail;
    }

    /**
     * @return the imDetailId
     */
    public String getImDetailId()
    {
        return imDetailId;
    }

    /**
     * @param imDetailId
     *            the imDetailId to set
     */
    public void setImDetailId(String imDetailId)
    {
        this.imDetailId = imDetailId;
    }

    /**
     * @return the imType
     */
    public String getImType()
    {
        return imType;
    }

    /**
     * @param imType
     *            the imType to set
     */
    public void setImType(String imType)
    {
        this.imType = imType;
    }

    /**
     * @return the imDetail
     */
    public String getImDetail()
    {
        return imDetail;
    }

    /**
     * @param imDetail
     *            the imDetail to set
     */
    public void setImDetail(String imDetail)
    {
        this.imDetail = imDetail;
    }

}
