package de.hhu.bsinfo.dxapp.chunk;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

import java.lang.reflect.Array;
import java.util.Arrays;

public class Vertex extends AbstractChunk {
    //private int[] m_inEdges = new int[0];
    private long[] m_inEdges = new long[0];
    private double[] m_pageRank = new double[2];
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
        m_pageRank[0] = 1/(double) N;
        m_pageRank[1] = 1/(double) N;
        if(m_outDeg == 0){
            m_outDeg = N - 1;
        }
    }

    public void increment_outDeg(int p_num, int N){
        if(m_outDeg  == N - 1){
            m_outDeg = 0;
        }
        m_outDeg += p_num;

    }

    public void setOutDeg(int p_outDeg){
        m_outDeg = p_outDeg;
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

    public void addInEdges(final long[] p_neighbors, boolean p_cut){
        if(p_cut){
            m_inEdges = Arrays.copyOf(p_neighbors, p_neighbors.length - 1);
        } else {
            m_inEdges = p_neighbors;
        }
    }

    public void calcPageRank(int N, double D, double p_sum, int p_round){
        m_pageRank[p_round] = (1 - D)/N + D * p_sum;
    }

    public int getOutDeg(){return m_outDeg;}

    public double getPageRank(int p_round){
        return m_pageRank[p_round];
    }

    public long[] getM_inEdges(){
        return m_inEdges;
    }


    public int get_name(){
        return m_name;
    }

    @Override
    public void exportObject(Exporter p_exporter) {
        p_exporter.writeDoubleArray(m_pageRank);
        p_exporter.writeInt(m_outDeg);
        p_exporter.writeInt(m_name);
        p_exporter.writeLongArray(m_inEdges);
    }

    @Override
    public void importObject(Importer p_importer) {
        m_pageRank = p_importer.readDoubleArray(m_pageRank);
        m_outDeg = p_importer.readInt(m_outDeg);
        m_name = p_importer.readInt(m_name);
        m_inEdges = p_importer.readLongArray(m_inEdges);
    }

    @Override
    public int sizeofObject() {
        return ObjectSizeUtil.sizeofDoubleArray(m_pageRank) + Integer.BYTES * 2 + ObjectSizeUtil.sizeofLongArray(m_inEdges);
    }
}
