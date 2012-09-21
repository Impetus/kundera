/*******************************************************************************
 * * Copyright 2012 Impetus Infotech.
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
package com.impetus.kundera.metadata.entities;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.JoinTable;
import javax.persistence.ManyToMany;
import javax.persistence.OneToMany;

/**
 * @author vivek.mishra
 * 
 */
@Entity
public class PluralOwnerType
{
    @Id
    @Column(name = "P_KEY")
    private Double primaryKey;

    @OneToMany
    @Column(name = "set_type")
    private Set<SetTypeAssociationEntity> setAssocition;

    @ManyToMany
    @JoinTable(name = "OWNER_LIST", joinColumns = { @JoinColumn(name = "P_KEY") }, inverseJoinColumns = { @JoinColumn(name = "listKey") })
    @Column(name = "list_type")
    private List<ListTypeAssociationEntity> listAssociation;

    @OneToMany
    @Column(name = "col_type")
    private Collection<CollectionTypeAssociationEntity> collectionAssociation;

    @Column(name = "map_type")
    @OneToMany
    private Map<Integer, MapTypeAssociationEntity> mapAssociation;

    /**
     * @return the primaryKey
     */
    public Double getPrimaryKey()
    {
        return primaryKey;
    }

    /**
     * @param primaryKey
     *            the primaryKey to set
     */
    public void setPrimaryKey(Double primaryKey)
    {
        this.primaryKey = primaryKey;
    }

    /**
     * @return the setAssocition
     */
    public Set<SetTypeAssociationEntity> getSetAssocition()
    {
        return setAssocition;
    }

    /**
     * @param setAssocition
     *            the setAssocition to set
     */
    public void setSetAssocition(Set<SetTypeAssociationEntity> setAssocition)
    {
        this.setAssocition = setAssocition;
    }

    /**
     * @return the listAssociation
     */
    public List<ListTypeAssociationEntity> getListAssociation()
    {
        return listAssociation;
    }

    /**
     * @param listAssociation
     *            the listAssociation to set
     */
    public void setListAssociation(List<ListTypeAssociationEntity> listAssociation)
    {
        this.listAssociation = listAssociation;
    }

    /**
     * @return the collectionAssociation
     */
    public Collection<CollectionTypeAssociationEntity> getCollectionAssociation()
    {
        return collectionAssociation;
    }

    /**
     * @param collectionAssociation
     *            the collectionAssociation to set
     */
    public void setCollectionAssociation(Collection<CollectionTypeAssociationEntity> collectionAssociation)
    {
        this.collectionAssociation = collectionAssociation;
    }

    /**
     * @return the mapAssociation
     */
    public Map<Integer, MapTypeAssociationEntity> getMapAssociation()
    {
        return mapAssociation;
    }

    /**
     * @param mapAssociation
     *            the mapAssociation to set
     */
    public void setMapAssociation(Map<Integer, MapTypeAssociationEntity> mapAssociation)
    {
        this.mapAssociation = mapAssociation;
    }

}
