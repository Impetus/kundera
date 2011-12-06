package com.impetus.client.manytoone;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;


@Entity
@Table(name="PERSON", schema="test")
public class MTONPerson
{
    @Id   
    @Column(name="PERSON_ID")    
    private String personId;
    
    @Column(name="PERSON_NAME")
    private String personName;
    
    @ManyToOne
    @JoinColumn(name = "ADDRESS_ID")
    private MTOAddress address;

    
    public String getPersonId()
    {
        return personId;
    }   
    

    public String getPersonName()
    {
        return personName;
    }

    public void setPersonName(String personName)
    {
        this.personName = personName;
    }



    public void setPersonId(String personId)
    {
        this.personId = personId;
    }


    public MTOAddress getAddress()
    {
        return address;
    }

    /**
     * @param address the address to set
     */
    public void setAddress(MTOAddress address)
    {
        this.address = address;
    }

}
