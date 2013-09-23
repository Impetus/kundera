package com.impetus.client.cassandra.thrift;

import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.Table;

@Entity
@Table(name = "PHONE", schema = "CompositeCassandra@composite_pu")
public class Phone
{
    @EmbeddedId
    private PhoneId phoneIdentifier;

    @Column
    private Long phoneNumber;

    public PhoneId getPhoneId()
    {
        return phoneIdentifier;
    }

    public void setPhoneId(PhoneId phoneId)
    {
        this.phoneIdentifier = phoneId;
    }

    public Long getPhoneNumber()
    {
        return phoneNumber;
    }

    public void setPhoneNumber(Long phoneNumber)
    {
        this.phoneNumber = phoneNumber;
    }

}
