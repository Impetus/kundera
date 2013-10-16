package com.impetus.client.couchdb.entities;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

import com.impetus.kundera.index.Index;
import com.impetus.kundera.index.IndexCollection;

@Entity
@Table(name = "tokens", schema = "couchdatabase@couchdb_pu")
@IndexCollection(columns = { @Index(name = "tokenName") })
public class CouchDBToken
{
    @Id
    @Column(name = "token_id")
    private String id;

    @ManyToOne(cascade = { CascadeType.ALL }, fetch = FetchType.EAGER)
    @JoinColumn(name = "client_id")
    private CouchDBTokenClient client;

    @Column
    private String tokenName;

    public String getId()
    {
        return id;
    }

    public void setId(String id)
    {
        this.id = id;
    }

    public CouchDBTokenClient getClient()
    {
        return client;
    }

    public void setClient(CouchDBTokenClient client)
    {
        this.client = client;
    }

    public String getTokenName()
    {
        return tokenName;
    }

    public void setTokenName(String tokenName)
    {
        this.tokenName = tokenName;
    }

}