/**
 * Copyright 2013 Impetus Infotech.
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
package com.impetus.client.crud.datatypes.entities;

import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * @author vivek.mishra
 * Entity with collection attribute.
 *
 */
@Entity
@Table(name = "foo_table")
public class EntityWithCollection
{
    @Id
    private String id;

    @Column(name = "dm")
    private Map<String, byte[]> dataMap;

    private Set<byte[]> setAsBytes;

    private List<byte[]> listAsBytes;

    public EntityWithCollection()
    {
    }

    public String getId()
    {
        return id;
    }

    public void setId(String id)
    {
        this.id = id;
    }

    public Map<String, byte[]> getDataMap()
    {
        return dataMap;
    }

    public void setDataMap(Map<String, byte[]> dataMap)
    {
        this.dataMap = dataMap;
    }

    public Set<byte[]> getSetAsBytes()
    {
        return setAsBytes;
    }

    public void setSetAsBytes(Set<byte[]> setAsBytes)
    {
        this.setAsBytes = setAsBytes;
    }

    public List<byte[]> getListAsBytes()
    {
        return listAsBytes;
    }

    public void setListAsBytes(List<byte[]> listAsBytes)
    {
        this.listAsBytes = listAsBytes;
    }

}
