package de.hhu.bsinfo.dxapp.chunk;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;

public class LocalDanglingChunks extends AbstractChunk {

    private long[] m_localDanglingChunks;

    public LocalDanglingChunks(){
        super();
    }

    public LocalDanglingChunks(long p_id){
        super(p_id);
    }

    public LocalDanglingChunks(long[] p_chunks){
        m_localDanglingChunks = p_chunks;
    }


    @Override
    public void exportObject(Exporter p_exporter) {

    }

    @Override
    public void importObject(Importer p_importer) {

    }

    @Override
    public int sizeofObject() {
        return 0;
    }
}
