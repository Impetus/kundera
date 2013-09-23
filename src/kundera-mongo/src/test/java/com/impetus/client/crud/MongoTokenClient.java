package com.impetus.client.crud;

import java.util.Set;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.Table;

@Entity
@Table(name = "client", schema = "KunderaExamples@mongoTest")
public class MongoTokenClient
{

    @Id
    @Column(name = "client_id")
    private String clientId;

    @Column(name = "client_name")
    private String clientName;

    @OneToMany(mappedBy = "client", fetch = FetchType.EAGER)
    private Set<MongoToken> tokens;

    public String getId()
    {
        return clientId;
    }

    public void setId(String id)
    {
        this.clientId = id;
    }

    public String getClientName()
    {
        return clientName;
    }

    public void setClientName(String clientName)
    {
        this.clientName = clientName;
    }

    public Set<MongoToken> getTokens()
    {
        return tokens;
    }

    public void setTokens(Set<MongoToken> tokens)
    {
        this.tokens = tokens;
    }
}