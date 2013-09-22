package com.impetus.kundera.metadata.entities;

import java.util.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

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

/**
 * @author vivek.mishra
 * 
 */
@Entity
@Table(name = "table", schema = "testSchema@keyspace")
public class SubClassB extends MappedSuperClass
{
    @Id
    private String subClassBStr;

    @Column
    private Date subClassBDt;

    @Column
    private long subClassBLng;

    /**
     * @return the subClassBStr
     */
    public String getSubClassBStr()
    {
        return subClassBStr;
    }

    /**
     * @param subClassBStr
     *            the subClassBStr to set
     */
    public void setSubClassBStr(String subClassBStr)
    {
        this.subClassBStr = subClassBStr;
    }

    /**
     * @return the subClassBDt
     */
    public Date getSubClassBDt()
    {
        return subClassBDt;
    }

    /**
     * @param subClassBDt
     *            the subClassBDt to set
     */
    public void setSubClassBDt(Date subClassBDt)
    {
        this.subClassBDt = subClassBDt;
    }

    /**
     * @return the subClassBLng
     */
    public long getSubClassBLng()
    {
        return subClassBLng;
    }

    /**
     * @param subClassBLng
     *            the subClassBLng to set
     */
    public void setSubClassBLng(long subClassBLng)
    {
        this.subClassBLng = subClassBLng;
    }

}
