package com.impetus.client.cassandra.thrift;

import javax.persistence.Column;
import javax.persistence.Embeddable;

@Embeddable
public class PhoneId
{

    @Column
    private String personId;

    @Column
    private String phoneId;

    public String getPersonId()
    {
        return personId;
    }

    public void setPersonId(String personId)
    {
        this.personId = personId;
    }

    public String getPhoneId()
    {
        return phoneId;
    }

    public void setPhoneId(String phoneId)
    {
        this.phoneId = phoneId;
    }

}
