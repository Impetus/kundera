package com.impetus.client.crud;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "client", schema = "myapp@myapp_pu")
public class TokenClient
{

    @Id
    @Column(name = "client_id")
    private String id;
    @Column(name = "client_name")
    private String clientName;
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
    
    
    
}
