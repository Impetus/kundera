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
@Table(name = "tokens", schema = "myapp@myapp_pu")
public class Token
{
    @Id
    @Column(name = "token_id")
    private String id;

    @ManyToOne(cascade = { CascadeType.ALL }, fetch = FetchType.EAGER)
    @JoinColumn(name = "client_id")
    private TokenClient client;

    public String getId()
    {
        return id;
    }

    public void setId(String id)
    {
        this.id = id;
    }

    public TokenClient getClient()
    {
        return client;
    }

    public void setClient(TokenClient client)
    {
        this.client = client;
    }

}