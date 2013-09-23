package com.impetus.kundera.metadata.validator;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;

@Entity
@Table(name = "GeneratedIdWithSequenceGenerator", schema = "KunderaTest@GeneratedValue")
public class GeneratedIdWithSequenceGenerator
{
    @Id
    @SequenceGenerator(name = "id_gen", allocationSize = 20, initialValue = 80, schema = "KunderaTest", sequenceName = "newSequence")
    @GeneratedValue(generator = "id_gen", strategy = GenerationType.SEQUENCE)
    private int id;

    @Column
    private String name;

    /**
     * @return the id
     */
    public int getId()
    {
        return id;
    }

    /**
     * @param id
     *            the id to set
     */
    public void setId(int id)
    {
        this.id = id;
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