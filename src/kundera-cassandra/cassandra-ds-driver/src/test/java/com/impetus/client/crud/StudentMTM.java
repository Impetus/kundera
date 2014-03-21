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
package com.impetus.client.crud;

import java.util.HashSet;
import java.util.Set;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.JoinTable;
import javax.persistence.ManyToMany;
import javax.persistence.Table;

/**
 * Student entity with many to many association
 * 
 * @author vivek.mishra
 *
 */
@Entity
@Table(name = "Student")
public class StudentMTM
{

    @Id
    @Column(name = "STUDENT_ID")
    private long studenId;

    @Column(name = "fname")
    private String firstName;

    @Column(name = "lname")
    private String lastName;

    @ManyToMany(cascade = CascadeType.ALL, fetch = FetchType.LAZY)
    @JoinTable(name = "STUDENT_CLASS", schema = "KunderaExamples", joinColumns = { @JoinColumn(name = "STUDENT_ID") }, inverseJoinColumns = { @JoinColumn(name = "CLASS_ID") })
    private Set<ClassMTM> classes = new HashSet<ClassMTM>();
    
    public StudentMTM()
    {
    }

    public StudentMTM(long studenId)
    {
        this.studenId = studenId;
    }

    public String getFirstName()
    {
        return firstName;
    }

    public void setFirstName(String firstName)
    {
        this.firstName = firstName;
    }

    public String getLastName()
    {
        return lastName;
    }

    public void setLastName(String lastName)
    {
        this.lastName = lastName;
    }

    public long getStudenId()
    {
        return studenId;
    }

    public Set<ClassMTM> getClasses()
    {
        return classes;
    }


    public void assignClass(ClassMTM studentClass)
    {
        this.classes.add(studentClass);
    }
}
