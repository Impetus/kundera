/**
 * Copyright 2013 Impetus Infotech.
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
package com.impetus.client.generatedId.entites;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import javax.persistence.TableGenerator;

@Entity
@Table(name = "EmployeeInfo", schema = "kunderaGeneratedId@cassandra_generated_id")
public class EmployeeInfo
{
    @Id
    @Column(name = "UserID")
    @TableGenerator(name = "id_gen", allocationSize = 1, initialValue = 1)
    @GeneratedValue(generator = "id_gen", strategy = GenerationType.TABLE)
    private Long userid;
    
//    @Column(name="name")
//    
//    private String employeeName;
    
    public EmployeeInfo()
    {
        
    }
    
    @ManyToOne(cascade = CascadeType.ALL, fetch = FetchType.LAZY)
    @JoinColumn(name = "address_id")
    private EmployeeAddress address;

    public Long getUserid()
    {
        return userid;
    }

    public void setUserid(Long userid)
    {
        this.userid = userid;
    }

    public EmployeeAddress getAddress()
    {
        return address;
    }

    public void setAddress(EmployeeAddress address)
    {
        this.address = address;
    }

//    public String getEmployeeName()
//    {
//        return employeeName;
//    }
//
//    public void setEmployeeName(String employeeName)
//    {
//        this.employeeName = employeeName;
//    }
//    
    
}
