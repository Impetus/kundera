package com.impetus.kundera.client.crud.inheritence;

import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.MappedSuperclass;
import javax.persistence.TableGenerator;



@MappedSuperclass
abstract public class GuidDomainObject 
{
   /* @Id
    @GeneratedValue
    @Column(name = "guid", updatable = false, nullable = false)
    private String id;*/
    
    @Id
//    @TableGenerator(name = "AEntity_id_generatorStrategy", table = "SequenceEntity", allocationSize = 1000)
//    @GeneratedValue(strategy = GenerationType.TABLE, generator = "AEntity_id_generatorStrategy")
    private Long id;

    public Long getId()
    {
        return id;
    }

    public void setId(Long id)
    {
        this.id = id;
    }

    
}
