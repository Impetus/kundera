package com.impetus.client.hbase.crud.embedded;

import java.io.Serializable;
import java.util.Date;

import javax.persistence.Column;
import javax.persistence.Embeddable;

@SuppressWarnings("serial")
@Embeddable
public class NetstatDataId implements Serializable
{

    @Column(name = "SERVER_ID", nullable = true)
    private String server;

    @Column(name = "CPTR_TS", nullable = true)
    private Date captureTime;

    @Column(name = "PORTMAP_ID", nullable = true)
    private Integer portMapId;

    @Column(name = "PORTMAP_TYP_CD", nullable = true)
    private String portMapTypeCd;

    public String getServer()
    {
        return this.server;
    }

    public void setServer(String server)
    {
        this.server = server;
    }

    public Date getCaptureTime()
    {
        return this.captureTime;
    }

    public void setCaptureTime(Date captureTime)
    {
        this.captureTime = captureTime;
    }

    public Integer getPortMapId()
    {
        return portMapId;
    }

    public void setPortMapId(Integer portMapId)
    {
        this.portMapId = portMapId;
    }

    public String getPortMapTypeCd()
    {
        return portMapTypeCd;
    }

    public void setPortMapTypeCd(String portMapTypeCd)
    {
        this.portMapTypeCd = portMapTypeCd;
    }

    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (o == null)
            return false;
        if (!(o instanceof NetstatDataId))
            return false;
        NetstatDataId that = (NetstatDataId) o;

        return ((server == that.server) || (server != null && that.server != null && server.equals(that.server)))
                && ((captureTime == that.captureTime) || (captureTime != null && that.captureTime != null && captureTime
                        .equals(that.captureTime)))
                && ((portMapId == that.portMapId) || (portMapId != null && that.portMapId != null && this.portMapId
                        .equals(that.portMapId))) && ((portMapTypeCd == that.portMapTypeCd));
    }

    public int hashCode()
    {
        int result = 17;
        result = 37 * result + (server == null ? 0 : server.hashCode());
        result = 37 * result + (captureTime == null ? 0 : captureTime.hashCode());
        result = 37 * result + (portMapId == null ? 0 : portMapId.hashCode());
        result = 37 * result + (portMapTypeCd == null ? 0 : portMapTypeCd.hashCode());
        return result;
    }

}
