package de.hhu.bsinfo.dxapp.chunk;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import jdk.nashorn.internal.objects.annotations.Getter;

public class VoteChunk extends AbstractChunk {

    private int m_votes;
    private double m_PRsum;

    public VoteChunk(){
        super();
        m_votes = 0;
        m_PRsum = 0.0;
    }

    public VoteChunk(int p_votes, double p_PRsum) {
        super();
        m_votes = p_votes;
        m_PRsum = p_PRsum;
    }

    public VoteChunk(long p_chunkID) {
        super(p_chunkID);
    }

    public int getVotes() {
        return m_votes;
    }

    public double getPRsum () {
        return m_PRsum;
    }

    public void incPrSum (double p_PRsum){
        m_PRsum += p_PRsum;
    }

    public void reset (){
        m_votes = 0;
        m_PRsum = 0.0;
    }

    public void incVotes(int p_votes) {
        m_votes += p_votes;
    }

    @Override
    public void exportObject(Exporter p_exporter) {
        p_exporter.writeInt(m_votes);
        p_exporter.writeDouble(m_PRsum);
    }

    @Override
    public void importObject(Importer p_importer) {
        m_votes = p_importer.readInt(m_votes);
        m_PRsum = p_importer.readDouble(m_PRsum);
    }

    @Override
    public int sizeofObject() {
        return Integer.BYTES + Double.BYTES;
    }
}
