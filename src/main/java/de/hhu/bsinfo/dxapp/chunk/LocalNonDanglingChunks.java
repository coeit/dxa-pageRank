package de.hhu.bsinfo.dxapp.chunk;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;

public class LocalNonDanglingChunks extends AbstractChunk {

    private long[] m_localNonDanglingChunks;

    public LocalNonDanglingChunks(){
        super();
    }

    public LocalNonDanglingChunks(long p_id){
        super(p_id);
    }

    public LocalNonDanglingChunks(long[] p_chunks){
        m_localNonDanglingChunks = p_chunks;
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
