package com.impetus.kundera.metadata.entities;

import java.util.Date;

import javax.persistence.AttributeOverride;
import javax.persistence.AttributeOverrides;
import javax.persistence.Column;
import javax.persistence.Entity;
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
@AttributeOverrides(value = { @AttributeOverride(name = "id", column = @Column(name = "ID_ATTRIB")),
        @AttributeOverride(name = "mappedFloat", column = @Column(name = "MAPPED_FLOAT")) })
public class EntityWithIdAttributeOverride extends IdMappedSuperClass
{

    @Column
    private Date subClassADt;

    @Column
    private long subClassALng;


    /**
     * @return the subClassADt
     */
    public Date getSubClassADt()
    {
        return subClassADt;
    }

    /**
     * @param subClassADt
     *            the subClassADt to set
     */
    public void setSubClassADt(Date subClassADt)
    {
        this.subClassADt = subClassADt;
    }

    /**
     * @return the subClassALng
     */
    public long getSubClassALng()
    {
        return subClassALng;
    }

    /**
     * @param subClassALng
     *            the subClassALng to set
     */
    public void setSubClassALng(long subClassALng)
    {
        this.subClassALng = subClassALng;
    }

}
