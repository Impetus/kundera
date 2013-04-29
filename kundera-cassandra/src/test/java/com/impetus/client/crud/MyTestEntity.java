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
package com.impetus.client.crud;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.UUID;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import com.impetus.kundera.index.Index;
import com.impetus.kundera.index.IndexCollection;

@Entity
@Table(name = "test1", schema = "KunderaCassandraXmlTest@CassandraXmlPropertyTest")
@IndexCollection(columns = { @Index(name = "url") })
public class MyTestEntity implements Serializable
{

    private static final long serialVersionUID = 1L;

    @Id
    @Column(name = "id")
    private UUID key;

    @Column(name = "userid")
    private UUID userid;

    @Column(name = "url")
    private String url;

    @Column(name = "datetime")
    private Timestamp datetime;

    @Column(name = "linkcounts")
    private int linkcounts;

    public MyTestEntity()
    {
    }

    public UUID getKey()
    {
        return key;
    }

    public void setKey(UUID key)
    {
        this.key = key;
    }

    public UUID getUserid()
    {
        return userid;
    }

    public void setUserid(UUID userid)
    {
        this.userid = userid;
    }

    public String getUrl()
    {
        return url;
    }

    public void setUrl(String url)
    {
        this.url = url;
    }

    public Timestamp getDatetime()
    {
        return datetime;
    }

    public void setDatetime(Timestamp datetime)
    {
        this.datetime = datetime;
    }

    public int getLinkcounts()
    {
        return linkcounts;
    }

    public void setLinkcounts(int linkcounts)
    {
        this.linkcounts = linkcounts;
    }
}
