package com.impetus.client.crud;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

@Entity
@Table(name = "tokens", schema = "KunderaExamples@mongoTest")
public class MongoToken
{
    @Id
    @Column(name = "token_id")
    private String tokenId;
    
    @Column
    private String tokenName;

    @ManyToOne(cascade = { CascadeType.ALL }, fetch = FetchType.EAGER)
    @JoinColumn(name = "client_id")
    private MongoTokenClient client;

    public String getId()
    {
        return tokenId;
    }

    public void setId(String id)
    {
        this.tokenId = id;
    }

    public MongoTokenClient getClient()
    {
        return client;
    }

    public void setClient(MongoTokenClient client)
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