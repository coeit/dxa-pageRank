package de.hhu.bsinfo.dxapp.chunk;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

import jdk.nashorn.internal.objects.annotations.Getter;

public class VoteChunk extends AbstractChunk {

    private double m_PRerr;
    private double m_PRsum;
    private long m_edgeCnt;

    public VoteChunk(int p_vertexCnt, long p_edgeCnt){
        super();
        m_PRerr = 0.0;
        m_PRsum = 1/(double) p_vertexCnt;
        m_edgeCnt = 0;
    }

    public VoteChunk(long p_chunkID) {
        super(p_chunkID);
    }

    public double getPRerr() {
        return m_PRerr;
    }

    public double getPRsum () {
        return m_PRsum;
    }

    public void incPRsum (double p_PRsum){
        m_PRsum += p_PRsum;
    }

    public void setPRsum (double p_PRsum) {
        m_PRsum = p_PRsum;
    }

    public void setPRerr (double p_PRerr) {
        m_PRerr = p_PRerr;
    }

    public void incPRerr(double p_PRerr) {
        m_PRerr += p_PRerr;
    }

    public void resetErr(){
        m_PRerr = 0.0;
    }

    public void resetSum(){
        m_PRsum = 0.0;
    }

    public long getEdgeCnt() {
        return m_edgeCnt;
    }

    public void setEdgeCnt(long p_edges){

    }

    @Override
    public void exportObject(Exporter p_exporter) {
        p_exporter.writeDouble(m_PRerr);
        p_exporter.writeDouble(m_PRsum);
        p_exporter.writeLong(m_edgeCnt);
    }

    @Override
    public void importObject(Importer p_importer) {
        m_PRerr = p_importer.readDouble(m_PRerr);
        m_PRsum = p_importer.readDouble(m_PRsum);
        m_edgeCnt = p_importer.readLong(m_edgeCnt);
    }

    @Override
    public int sizeofObject() {
        return Double.BYTES * 2 + Long.BYTES;
    }
}
