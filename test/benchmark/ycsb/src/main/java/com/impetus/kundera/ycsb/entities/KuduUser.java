/**
 * Copyright 2017 Impetus Infotech.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.impetus.kundera.ycsb.entities;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * @author karthikp.manchala
 * 
 */
@Entity
@Table(name = "kuduuser", schema = "kundera@kundera_kudu_pu")
public class KuduUser
{

    @Id
    private String name;

    @Column
    private String age;

    @Column
    private String address;

    @Column
    private String lname;

    public KuduUser()
    {

    }

    public KuduUser(String name, String age, String add, String lname)
    {
        this.lname = lname;
        this.address = add;
        this.name = name;
        this.age = age;
    }

    /**
     * @return the name
     */
    public String getName()
    {
        return name;
    }

    /**
     * @param name
     *            the name to set
     */
    public void setName(String name)
    {
        this.name = name;
    }

    /**
     * @return the age
     */
    public String getAge()
    {
        return age;
    }

    /**
     * @param age
     *            the age to set
     */
    public void setAge(String age)
    {
        this.age = age;
    }

    /**
     * @return the address
     */
    public String getAddress()
    {
        return address;
    }

    /**
     * @param address
     *            the address to set
     */
    public void setAddress(String address)
    {
        this.address = address;
    }

    /**
     * @return the last name
     */
    public String getLname()
    {
        return lname;
    }

    /**
     * @param lname
     *            the last name to set
     */
    public void setLname(String lname)
    {
        this.lname = lname;
    }

}
