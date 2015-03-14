/*******************************************************************************
 * * Copyright 2015 Impetus Infotech.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 ******************************************************************************/
 /*
 * author: karthikp.manchala
 */
package com.impetus.client.cassandra.udt;

import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.Column;
import javax.persistence.ElementCollection;
import javax.persistence.Embedded;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import com.impetus.kundera.index.Index;
import com.impetus.kundera.index.IndexCollection;

/**
 * The Class PersonUDT.
 */
@Entity
@Table(name = "person_udt", schema = "KunderaExamples@ds_pu")
@IndexCollection(columns = { @Index(name = "nicknames"), @Index(name = "email"), @Index(name = "password") })
public class PersonUDT
{

    /** The person id. */
    @Id
    @Column
    private String personId;

    /** The personal details. */
    @Embedded
    private PersonalDetailsUDT personalDetails;

    /** The professional details. */
    @Embedded
    private ProfessionalDetailsUDT professionalDetails;

    /** The email. */
    @Column
    private String email;

    /** The password. */
    @Column
    private String password;

    /** The nicknames. */
    @Column
    private List<String> nicknames;
    
    /** The list profs. */
    @ElementCollection
    private List<ProfessionalDetailsUDT> listProfs;
    
    /** The set profs. */
    @ElementCollection
    private Set<ProfessionalDetailsUDT> setProfs;
    
    /** The map profs key. */
    @ElementCollection
    private Map<String, ProfessionalDetailsUDT> mapProfsKey;
    
    
    /**
     * Gets the sets the profs.
     *
     * @return the sets the profs
     */
    public Set<ProfessionalDetailsUDT> getSetProfs()
    {
        return setProfs;
    }

    /**
     * Sets the sets the profs.
     *
     * @param setProfs the new sets the profs
     */
    public void setSetProfs(Set<ProfessionalDetailsUDT> setProfs)
    {
        this.setProfs = setProfs;
    }

    /**
     * Gets the map profs key.
     *
     * @return the map profs key
     */
    public Map<String, ProfessionalDetailsUDT> getMapProfsKey()
    {
        return mapProfsKey;
    }

    /**
     * Sets the map profs key.
     *
     * @param mapProfsKey the map profs key
     */
    public void setMapProfsKey(Map<String, ProfessionalDetailsUDT> mapProfsKey)
    {
        this.mapProfsKey = mapProfsKey;
    }

//    public Map<Fullname, ProfessionalDetailsUDT> getMapProfsBoth()
//    {
//        return mapProfsBoth;
//    }
//
//    public void setMapProfsBoth(Map<Fullname, ProfessionalDetailsUDT> mapProfsBoth)
//    {
//        this.mapProfsBoth = mapProfsBoth;
//    }

//    public Map<ProfessionalDetailsUDT, Integer> getMapProfsValue()
//    {
//        return mapProfsValue;
//    }
//
//    public void setMapProfsValue(Map<ProfessionalDetailsUDT, Integer> mapProfsValue)
//    {
//        this.mapProfsValue = mapProfsValue;
//    }
//
//    @ElementCollection
//    private Map<ProfessionalDetailsUDT, Integer> mapProfsValue;

    
    /**
 * Gets the list profs.
 *
 * @return the list profs
 */
public List<ProfessionalDetailsUDT> getListProfs()
    {
        return listProfs;
    }

    /**
     * Sets the list profs.
     *
     * @param listProfs the new list profs
     */
    public void setListProfs(List<ProfessionalDetailsUDT> listProfs)
    {
        this.listProfs = listProfs;
    }

    /**
     * Gets the person id.
     * 
     * @return the person id
     */
    public String getPersonId()
    {
        return personId;
    }

    /**
     * Sets the person id.
     * 
     * @param personId
     *            the new person id
     */
    public void setPersonId(String personId)
    {
        this.personId = personId;
    }

    /**
     * Gets the personal details.
     * 
     * @return the personal details
     */
    public PersonalDetailsUDT getPersonalDetails()
    {
        return personalDetails;
    }

    /**
     * Sets the personal details.
     * 
     * @param personalDetails
     *            the new personal details
     */
    public void setPersonalDetails(PersonalDetailsUDT personalDetails)
    {
        this.personalDetails = personalDetails;
    }

    /**
     * Gets the professional details.
     * 
     * @return the professional details
     */
    public ProfessionalDetailsUDT getProfessionalDetails()
    {
        return professionalDetails;
    }

    /**
     * Sets the professional details.
     * 
     * @param professionalDetails
     *            the new professional details
     */
    public void setProfessionalDetails(ProfessionalDetailsUDT professionalDetails)
    {
        this.professionalDetails = professionalDetails;
    }

    /**
     * Gets the email.
     * 
     * @return the email
     */
    public String getEmail()
    {
        return email;
    }

    /**
     * Sets the email.
     * 
     * @param email
     *            the new email
     */
    public void setEmail(String email)
    {
        this.email = email;
    }

    /**
     * Gets the password.
     * 
     * @return the password
     */
    public String getPassword()
    {
        return password;
    }

    /**
     * Sets the password.
     * 
     * @param password
     *            the new password
     */
    public void setPassword(String password)
    {
        this.password = password;
    }

    /**
     * Gets the nicknames.
     * 
     * @return the nicknames
     */
    public List<String> getNicknames()
    {
        return nicknames;
    }

    /**
     * Sets the nicknames.
     * 
     * @param nicknames
     *            the new nicknames
     */
    public void setNicknames(List<String> nicknames)
    {
        this.nicknames = nicknames;
    }

}
