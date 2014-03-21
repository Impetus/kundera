package com.impetus.client.crud.entities;

import javax.persistence.Column;
import javax.persistence.Embeddable;

@Embeddable
public class EmbeddedEntity
{
    @Column(name = "EMAIL_ID", table = "SECONDARY_TABLE")
    private String emailId;

    @Column(name = "PHONE_NO")
    private long phoneNo;

    public String getEmailId()
    {
        return emailId;
    }

    public void setEmailId(String emailId)
    {
        this.emailId = emailId;
    }

    public long getPhoneNo()
    {
        return phoneNo;
    }

    public void setPhoneNo(long phoneNo)
    {
        this.phoneNo = phoneNo;
    }

}
