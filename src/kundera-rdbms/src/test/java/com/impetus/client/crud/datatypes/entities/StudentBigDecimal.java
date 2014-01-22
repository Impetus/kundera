package com.impetus.client.crud.datatypes.entities;

import java.math.BigDecimal;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import com.impetus.kundera.index.Index;
import com.impetus.kundera.index.IndexCollection;

@Entity
@Table(name = "StudentBigDecimal", schema = "TESTDB")
@IndexCollection(columns = { @Index(name = "age"), @Index(name = "name") })
public class StudentBigDecimal
{

    @Id
    @Column(name = "STUDENT_ID")
    private BigDecimal studentId;

    @Column(name = "AGE")
    private short age;

    @Column(name = "NAME")
    private String name;

    /**
     * @return the id
     */
    public BigDecimal getStudentId()
    {
        return studentId;
    }

    /**
     * @param id
     *            the id to set
     */
    public void setStudentId(BigDecimal studentId)
    {
        this.studentId = studentId;
    }

    /**
     * @return the age
     */
    public short getAge()
    {
        return age;
    }

    /**
     * @param age
     *            the age to set
     */
    public void setAge(short age)
    {
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

}
