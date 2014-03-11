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
import javax.persistence.ManyToMany;
import javax.persistence.Table;

/**
 * Class entity with many to many association.
 * 
 * @author vivek.mishra
 *
 */
@Entity
@Table(name="ClassMTM")
public class ClassMTM
{

    @Id
    @Column(name = "CLASS_ID")
    private long classId;

    @Column(name = "topic")
    private String topic;

    @Column(name = "classNo")
    private int classNumber;

    @ManyToMany(mappedBy = "classes", fetch = FetchType.LAZY, cascade=CascadeType.ALL)
    private Set<StudentMTM> students= new HashSet<StudentMTM>();

    public ClassMTM()
    {
    }
    
    public ClassMTM(long classId)
    {
        this.classId = classId;
    }

    public String getTopic()
    {
        return topic;
    }

    public void setTopic(String topic)
    {
        this.topic = topic;
    }

    public int getClassNumber()
    {
        return classNumber;
    }

    public void setClassNumber(int classNumber)
    {
        this.classNumber = classNumber;
    }

    public long getClassId()
    {
        return classId;
    }

    public Set<StudentMTM> getStudents()
    {
        return students;
    }

    public void assignStudent(StudentMTM student)
    {
        this.students.add(student);
    }

    
}
