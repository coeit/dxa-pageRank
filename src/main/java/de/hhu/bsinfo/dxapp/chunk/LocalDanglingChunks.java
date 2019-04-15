package de.hhu.bsinfo.dxapp.chunk;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

public class LocalDanglingChunks extends AbstractChunk {

    private long[] m_localDanglingChunks;

    public LocalDanglingChunks(){
        super();
    }

    public LocalDanglingChunks(long p_id){
        super(p_id);
    }

    public LocalDanglingChunks(long[] p_chunks){
        super();
        m_localDanglingChunks = p_chunks;
    }

    public long[] getLocalDanglingChunks() {
        return m_localDanglingChunks;
    }

    @Override
    public void exportObject(Exporter p_exporter) {
        p_exporter.writeLongArray(m_localDanglingChunks);
    }

    @Override
    public void importObject(Importer p_importer) {
        m_localDanglingChunks = p_importer.readLongArray(m_localDanglingChunks);
    }

    @Override
    public int sizeofObject() {
        return ObjectSizeUtil.sizeofLongArray(m_localDanglingChunks);
    }
}
