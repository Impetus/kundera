package com.impetus.kundera.datakeeper.entities;

import java.util.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

import org.primefaces.model.StreamedContent;

import com.impetus.kundera.datakeeper.utils.DataFormat;
import com.impetus.kundera.index.Index;
import com.impetus.kundera.index.IndexCollection;

/**
 * @author Kuldeep.Mishra
 * 
 */
@Entity
@Table(name = "DOCUMENT", schema = "datakeeper@mongo-pu")
@IndexCollection(columns = { @Index(name = "ownerName"), @Index(name = "ownerId"), @Index(name = "documentName") })
public class DocumentInfo
{
    @Id
    @Column(name = "DOCUMENT_ID")
    @GeneratedValue()
    private String dataId;

    @Column(name = "EMPLOYEE_NAME")
    private String ownerName;

    @Column(name = "EMPLOYEE_ID")
    private int ownerId;

    @Column(name = "UPLOADED_DATE")
    private Date uplodedDate;

    @Column(name = "DOCUMENT_FORMAT")
    private DataFormat dataFormat;

    @Column(name = "DOCUMENT_NAME")
    private String documentName;

    @Column(name = "DATA")
    private byte[] data;

    private StreamedContent file;

    @Column(name = "SIZE")
    private long size;

    /**
     * @return the dataId
     */
    public String getDataId()
    {
        return dataId;
    }

    /**
     * @param dataId
     *            the dataId to set
     */
    public void setDataId(String dataId)
    {
        this.dataId = dataId;
    }

    /**
     * @return the ownerName
     */
    public String getOwnerName()
    {
        return ownerName;
    }

    /**
     * @param ownerName
     *            the ownerName to set
     */
    public void setOwnerName(String ownerName)
    {
        this.ownerName = ownerName;
    }

    /**
     * @return the ownerId
     */
    public int getOwnerId()
    {
        return ownerId;
    }

    /**
     * @param ownerId
     *            the ownerId to set
     */
    public void setOwnerId(int ownerId)
    {
        this.ownerId = ownerId;
    }

    /**
     * @return the uplodedDate
     */
    public Date getUplodedDate()
    {
        return uplodedDate;
    }

    /**
     * @param uplodedDate
     *            the uplodedDate to set
     */
    public void setUplodedDate(Date uplodedDate)
    {
        this.uplodedDate = uplodedDate;
    }

    /**
     * @return the dataFormat
     */
    public DataFormat getDataFormat()
    {
        return dataFormat;
    }

    /**
     * @param dataFormat
     *            the dataFormat to set
     */
    public void setDataFormat(DataFormat dataFormat)
    {
        this.dataFormat = dataFormat;
    }

    public byte[] getData()
    {
        return data;
    }

    public void setData(byte[] data)
    {
        this.data = data;
    }

    public String getDocumentName()
    {
        return documentName;
    }

    public void setDocumentName(String documentName)
    {
        this.documentName = documentName;
    }

    public long getSize()
    {
        return size;
    }

    public void setSize(long size)
    {
        this.size = size;
    }

    public StreamedContent getFile()
    {
        return file;
    }

    public void addFile(StreamedContent file)
    {
        this.file = file;
    }

}
