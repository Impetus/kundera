/**
 * 
 */
package com.impetus.client.cassandra.thrift;

import java.util.List;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.OneToMany;
import javax.persistence.Table;

/**
 * @author impadmin
 * 
 */
@Entity
@Table(name = "PERSONIDENTITY", schema = "CompositeCassandra@composite_pu")
public class PersonIdentity
{
    @Id
    private String personId;

    @Column
    private String personName;

    @OneToMany(cascade=CascadeType.ALL,fetch=FetchType.EAGER)
    @JoinColumn(name = "personId")
    private List<Phone> phones;
    

  
    public String getPersonName()
    {
        return personName;
    }

    public void setPersonName(String personName)
    {
        this.personName = personName;
    }

  
    public String getPersonId()
    {
        return personId;
    }

    public void setPersonId(String personId)
    {
        this.personId = personId;
    }

    public List<Phone> getPhones()
    {
        return phones;
    }

    public void setPhones(List<Phone> phones)
    {
        this.phones = phones;
    }
    
    
}
