package de.hhu.bsinfo.dxapp.chunk;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

import jdk.nashorn.internal.objects.annotations.Getter;

public class VoteChunk extends AbstractChunk {

    private double m_PRerr;
    private double[] m_PRsum = new double[2];

    public VoteChunk(int p_vertexCnt){
        super();
        m_PRerr = 0.0;
        m_PRsum[0] = 1/(double) p_vertexCnt;
        m_PRsum[1] = 0.0;
    }

    public VoteChunk(long p_chunkID) {
        super(p_chunkID);
    }

    public double getPRerr() {
        return m_PRerr;
    }

    public double getPRsum (int p_round) {
        return m_PRsum[p_round];
    }

    public void incPRsum (double p_PRsum, int p_round){
        m_PRsum[p_round] += p_PRsum;
    }

    public void incPRerr(double p_PRerr) {
        m_PRerr += p_PRerr;
    }

    public void resetErr(){
        m_PRerr = 0.0;
    }

    public void resetSum(int p_round, double p_newDanglingPR){
        m_PRsum[Math.abs(p_round - 1)] = p_newDanglingPR;
        m_PRsum[p_round] = 0.0;
    }

    @Override
    public void exportObject(Exporter p_exporter) {
        p_exporter.writeDouble(m_PRerr);
        p_exporter.writeDoubleArray(m_PRsum);
    }

    @Override
    public void importObject(Importer p_importer) {
        m_PRerr = p_importer.readDouble(m_PRerr);
        m_PRsum = p_importer.readDoubleArray(m_PRsum);
    }

    @Override
    public int sizeofObject() {
        return Double.BYTES + ObjectSizeUtil.sizeofDoubleArray(m_PRsum);
    }
}
