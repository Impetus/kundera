package com.impetus.client.hbase.crud.embedded;

import java.io.Serializable;

import javax.persistence.Column;
import javax.persistence.Embedded;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@SuppressWarnings("serial")
@Entity
@Table(name = "NETSTAT_DTL_SMRY", schema = "KunderaExamples@hbaseTest")
public class NetstatData implements Serializable
{

    @Id
    @Column(name = "ROW_KEY")
    private String rowKey; // added for hbase

    @Embedded
    private NetstatDataId id;

    @Column(name = "ESTAB_PORT_CT", nullable = true)
    private Integer established;

    @Column(name = "CLOSE_WAIT_CT", nullable = true)
    private Integer closeWait;

    @Column(name = "FINISH_WAIT_CT", nullable = true)
    private Integer finWait;

    @Column(name = "FINISH_WAIT2_CT", nullable = true)
    private Integer finWait2;

    @Column(name = "IDLE_CT", nullable = true)
    private Integer idle;

    @Column(name = "LISTEN_CT", nullable = true)
    private Integer listen;

    @Column(name = "SYNC_RCV_CT", nullable = true)
    private Integer synRecv;

    @Column(name = "TIME_WAIT_CT", nullable = true)
    private Integer timeWait;

    @Column(name = "PORT_CT", nullable = true)
    private Integer total;

    public NetstatData()
    {
        NetstatDataId id = new NetstatDataId();
        this.id = id;
    }

    private transient String portDesc;

    public NetstatDataId getId()
    {
        return this.id;
    }

    public void setId(NetstatDataId id)
    {
        this.id = id;
    }

    public Integer getEstablished()
    {
        return this.established;
    }

    public void setEstablished(Integer established)
    {
        this.established = established;
    }

    public Integer getCloseWait()
    {
        return this.closeWait;
    }

    public void setCloseWait(Integer closeWait)
    {
        this.closeWait = closeWait;
    }

    public Integer getFinWait()
    {
        return this.finWait;
    }

    public void setFinWait(Integer finWait)
    {
        this.finWait = finWait;
    }

    public Integer getFinWait2()
    {
        return this.finWait2;
    }

    public void setFinWait2(Integer finWait2)
    {
        this.finWait2 = finWait2;
    }

    public Integer getIdle()
    {
        return this.idle;
    }

    public void setIdle(Integer idle)
    {
        this.idle = idle;
    }

    public Integer getListen()
    {
        return this.listen;
    }

    public void setListen(Integer listen)
    {
        this.listen = listen;
    }

    public Integer getSynRecv()
    {
        return this.synRecv;
    }

    public void setSynRecv(Integer synRecv)
    {
        this.synRecv = synRecv;
    }

    public Integer getTimeWait()
    {
        return this.timeWait;
    }

    public void setTimeWait(Integer timeWait)
    {
        this.timeWait = timeWait;
    }

    public Integer getTotal()
    {
        return this.total;
    }

    public void setTotal(Integer total)
    {
        this.total = total;
    }

    public String getPortDesc()
    {
        return portDesc;
    }

    public void setPortDesc(String portDesc)
    {
        this.portDesc = portDesc;
    }

    public String getPortType()
    {
        // For DataPower return PortMapTypeCd itself
        if (id.getPortMapTypeCd().length() > 1)
        {
            return id.getPortMapTypeCd();
        }

        if (id.getPortMapTypeCd() == null)
            return "Others";

        switch (id.getPortMapTypeCd().toCharArray()[0])
        {
        case 'A':
            return "App Port";
        case 'N':
            return "Netscaler";
        case 'W':
            return "Web Port";
        case 'Q':
            return "MQ";
        case 'D':
            return "DB";
        case 'L':
            return "Local";
        case '?':
            return "?";
        default:
            return "Others";
        }

    }

    public void setPortType(String s)
    {
        throw new IllegalArgumentException("Can't set a read-only attribute");
    }

    public String getRowKey()
    {
        return rowKey;
    }

    public void setRowKey(String rowKey)
    {
        this.rowKey = rowKey;
    }

}