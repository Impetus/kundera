package com.impetus.client.crud.entities;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

@Entity
@Table(name = "TOKENS", schema = "TESTDB")
public class RDBMSToken
{
    @Id
    @Column(name = "TOKEN_ID")
    private String tokenId;

    @Column(name = "TOKEN_NAME")
    private String tokenName;

    @ManyToOne(cascade = { CascadeType.ALL }, fetch = FetchType.EAGER)
    @JoinColumn(name = "client_id")
    private RDBMSTokenClient client;

    public String getId()
    {
        return tokenId;
    }

    public void setId(String id)
    {
        this.tokenId = id;
    }

    public RDBMSTokenClient getClient()
    {
        return client;
    }

    public void setClient(RDBMSTokenClient client)
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