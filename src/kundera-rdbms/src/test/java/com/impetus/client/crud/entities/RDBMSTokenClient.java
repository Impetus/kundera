package com.impetus.client.crud.entities;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "CLIENT", schema = "TESTDB")
public class RDBMSTokenClient
{

    @Id
    @Column(name = "CLIENT_ID")
    private String clientId;

    @Column(name = "CLIENT_NAME")
    private String clientName;

//    @OneToMany(mappedBy = "client", fetch = FetchType.LAZY)
//    private Set<RDBMSToken> tokens;

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

//    public Set<RDBMSToken> getTokens()
//    {
//        return tokens;
//    }
//
//    public void setTokens(Set<RDBMSToken> tokens)
//    {
//        this.tokens = tokens;
//    }
}