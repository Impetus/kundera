package com.impetus.kundera.metadata.validator;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;
import javax.persistence.TableGenerator;

@Entity
@Table(name = "GeneratedIdWithNoGenerator", schema = "KunderaTest@GeneratedValue")
@TableGenerator(name = "id_gen")
public class GeneratedIdWithInvalidGenerator
{
    @Id
    @SequenceGenerator(name = "seq_gen")
    @GeneratedValue(generator = "id")
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
