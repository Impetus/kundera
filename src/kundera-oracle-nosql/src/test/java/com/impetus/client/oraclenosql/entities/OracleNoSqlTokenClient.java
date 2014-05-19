/*******************************************************************************
 * * Copyright 2013 Impetus Infotech.
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
package com.impetus.client.oraclenosql.entities;

import java.util.Set;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.Table;

/**
 * @author vivek.mishra
 *
 */
@Entity
@Table(name = "client")
public class OracleNoSqlTokenClient
{

    @Id
    @Column(name = "client_id")
    private String id;

    @Column(name = "client_name")
    private String clientName;

    @OneToMany(mappedBy = "client", fetch = FetchType.EAGER)
    private Set<OracleNoSqlToken> tokens;

    public String getId()
    {
        return id;
    }

    public void setId(String id)
    {
        this.id = id;
    }

    public String getClientName()
    {
        return clientName;
    }

    public void setClientName(String clientName)
    {
        this.clientName = clientName;
    }

    public Set<OracleNoSqlToken> getTokens()
    {
        return tokens;
    }

    public void setTokens(Set<OracleNoSqlToken> tokens)
    {
        this.tokens = tokens;
    }

}
