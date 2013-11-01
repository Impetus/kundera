/**
 * 
 */
package com.impetus.client.crud;

import javax.persistence.Embeddable;
import javax.persistence.Embedded;

/**
 * @author impadmin
 * 
 */
@Embeddable
public class PersonalDetailEmbedded
{
    private long phoneNo;

    private String emailId;

    private String address;

    @Embedded
    private PhoneDirectory phone;

    public String getEmailId()
    {
        return emailId;
    }

    public void setEmailId(String emailId)
    {
        this.emailId = emailId;
    }

    public String getAddress()
    {
        return address;
    }

    public void setAddress(String address)
    {
        this.address = address;
    }

    public long getPhoneNo()
    {
        return phoneNo;
    }

    public void setPhoneNo(long phoneNo)
    {
        this.phoneNo = phoneNo;
    }

    public PhoneDirectory getPhone()
    {
        return phone;
    }

    public void setPhone(PhoneDirectory phone)
    {
        this.phone = phone;
    }
}