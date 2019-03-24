package de.hhu.bsinfo.dxapp.chunk;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

import java.util.Arrays;

public class Vertex extends AbstractChunk {
    //private int[] m_inEdges = new int[0];
    private long[] m_inEdges = new long[0];
    private double m_PR1 = 0.0;
    private double m_PR2 = 0.0;
    private int m_outDeg = 0;
    private int m_name;

    public Vertex(){
        super();
    }

    public Vertex(int p_name) {
        super();
        m_name = p_name;
    }

    public Vertex(final long p_id){
        super(p_id);
    }

    public void invokeVertexPR(int N) {
        m_PR1 = 1/(double) N;
        m_PR2 = 1/(double) N;
        if(m_outDeg == 0){
            m_outDeg = N;
        }
    }

    public void increment_outDeg(int p_num, int N){
        if(m_outDeg  == N){
            m_outDeg = 0;
        }
        m_outDeg += p_num;

    }

    public void addInEdge(final long p_neighbour) {
        setInCnt(m_inEdges.length + 1);
        m_inEdges[m_inEdges.length - 1] = p_neighbour;
    }

    public void setInCnt(final int p_cnt) {
        if (p_cnt != m_inEdges.length) {
            m_inEdges = Arrays.copyOf(m_inEdges, p_cnt);
        }

    }

    public void calcPR1(int N, double D, double p_tmpPR){
        m_PR1 = (1 - D)/N + D * p_tmpPR;
    }

    public void calcPR2(int N, double D, double p_tmpPR){
        m_PR2 = (1 - D)/N + D * p_tmpPR;
    }


    public int getOutDeg(){return m_outDeg;}

    public double getPR1(){
        return m_PR1;
    }

    public double getPR2(){
        return m_PR2;
    }

    public long[] getM_inEdges(){
        return m_inEdges;
    }


    public int get_name(){
        return m_name;
    }

    @Override
    public void exportObject(Exporter p_exporter) {
        p_exporter.writeDouble(m_PR1);
        p_exporter.writeDouble(m_PR2);
        p_exporter.writeInt(m_outDeg);
        p_exporter.writeInt(m_name);
        p_exporter.writeLongArray(m_inEdges);
    }

    @Override
    public void importObject(Importer p_importer) {
        m_PR1 = p_importer.readDouble(m_PR1);
        m_PR2 = p_importer.readDouble(m_PR2);
        m_outDeg = p_importer.readInt(m_outDeg);
        m_name = p_importer.readInt(m_name);
        m_inEdges = p_importer.readLongArray(m_inEdges);
    }

    @Override
    public int sizeofObject() {
        return Double.BYTES * 2 + Integer.BYTES * 2 + ObjectSizeUtil.sizeofLongArray(m_inEdges);
    }
}
