package com.impetus.client.onetomany;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;


@Entity
@Table(name="ADDRESS", schema="KunderaKeyspace@kcassandra")
public class OTMAddress
{
    @Id    
    @Column(name = "ADDRESS_ID")
    private String addressId;   
   

    @Column(name = "STREET")
    private String street;   
    
    @ManyToOne
    @JoinColumn(name="PERSON_ID")
    private OTMNPerson person; 
    
    	

	public String getAddressId() {
		return addressId;
	}

	public void setAddressId(String addressId) {
		this.addressId = addressId;
	}

	public String getStreet()
    {
        return street;
    }

    public void setStreet(String street)
    {
        this.street = street;
    }

    /**
     * @return the person
     */
    public OTMNPerson getPerson()
    {
        return person;
    }

    /**
     * @param person the person to set
     */
    public void setPerson(OTMNPerson person)
    {
        this.person = person;
    }  

    
}
