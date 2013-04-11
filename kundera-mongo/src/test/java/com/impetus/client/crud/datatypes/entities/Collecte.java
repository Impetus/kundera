/*******************************************************************************
 *  * Copyright 2013 Impetus Infotech.
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
package com.impetus.client.crud.datatypes.entities;

import java.util.Date;
import java.util.List;

import javax.persistence.CollectionTable;
import javax.persistence.Column;
import javax.persistence.ElementCollection;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

@Entity
@Table(name = "COLLECTE", schema = "KunderaExamples@mongoTest")
public class Collecte
{

    @Id
    @Column(name = "COLLECTE_ID")
    private String id;

    @Column(name = "EAN", nullable = false)
    private String EAN;

    @Column(name = "PRODUIT_ID")
    private Long idProduit;

    @Temporal(TemporalType.TIMESTAMP)
    @Column(name = "dateStatut")
    private Date dateStatut;

    @Column(name = "statut")
    private int statut;

    // Element collection, will persist co-located
    @ElementCollection(fetch = FetchType.LAZY)
    @CollectionTable(name = "PHOTOS")
    private List<Photoo> photos;

    public String getId()
    {
        return id;
    }

    public void setId(String id)
    {
        this.id = id;
    }

    public String getEAN()
    {
        return EAN;
    }

    public void setEAN(String eAN)
    {
        EAN = eAN;
    }

    public Long getIdProduit()
    {
        return idProduit;
    }

    public void setIdProduit(Long idProduit)
    {
        this.idProduit = idProduit;
    }

    public Date getDateStatut()
    {
        return dateStatut;
    }

    public void setDateStatut(Date dateStatut)
    {
        this.dateStatut = dateStatut;
    }

    public int getStatut()
    {
        return statut;
    }

    public void setStatut(int statut)
    {
        this.statut = statut;
    }

    public List<Photoo> getPhotos()
    {
        return photos;
    }

    public void setPhotos(List<Photoo> photos)
    {
        this.photos = photos;
    }

    
}