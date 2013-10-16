package com.impetus.client.couchdb.entities;

import java.util.Set;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.Table;

@Entity
@Table(name = "client", schema = "couchdatabase@couchdb_pu")
public class CouchDBTokenClient
{

    @Id
    @Column(name = "client_id")
    private String id;

    @Column(name = "client_name")
    private String clientName;

    @OneToMany(mappedBy = "client", fetch = FetchType.EAGER)
    private Set<CouchDBToken> tokens;

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

    public Set<CouchDBToken> getTokens()
    {
        return tokens;
    }

    public void setTokens(Set<CouchDBToken> tokens)
    {
        this.tokens = tokens;
    }

}
