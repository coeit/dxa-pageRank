package de.hhu.bsinfo.dxapp.chunk;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

public class LocalNonDanglingChunks extends AbstractChunk {

    private long[] m_localNonDanglingChunks;

    public LocalNonDanglingChunks(){
        super();
    }

    public LocalNonDanglingChunks(long p_id){
        super(p_id);
    }

    public LocalNonDanglingChunks(long[] p_chunks){
        super();
        m_localNonDanglingChunks = p_chunks;
    }

    public long[] getLocalNonDanglingChunks() {
        return m_localNonDanglingChunks;
    }

    @Override
    public void exportObject(Exporter p_exporter) {
        p_exporter.writeLongArray(m_localNonDanglingChunks);
    }

    @Override
    public void importObject(Importer p_importer) {
        m_localNonDanglingChunks = p_importer.readLongArray(m_localNonDanglingChunks);
    }

    @Override
    public int sizeofObject() {
        return ObjectSizeUtil.sizeofLongArray(m_localNonDanglingChunks);
    }
}
