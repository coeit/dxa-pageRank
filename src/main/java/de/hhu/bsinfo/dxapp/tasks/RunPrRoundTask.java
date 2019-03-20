package de.hhu.bsinfo.dxapp.tasks;

import de.hhu.bsinfo.dxapp.chunk.Vertex;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxmem.data.ChunkIDRanges;
import de.hhu.bsinfo.dxmem.data.ChunkLockOperation;
import de.hhu.bsinfo.dxmem.operations.Lock;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxapp.chunk.PageRankInVertex;
import de.hhu.bsinfo.dxram.ms.Signal;
import de.hhu.bsinfo.dxram.ms.Task;
import de.hhu.bsinfo.dxram.ms.TaskContext;
import de.hhu.bsinfo.dxram.nameservice.NameserviceEntryStr;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxutils.NodeID;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class RunPrRoundTask implements Task {

    //private int NUM_THREADS;
    private int N;
    private double DAMP;
    private boolean m_flag;

    public RunPrRoundTask(){}

    public RunPrRoundTask(int vertexCount, double damping_factor, boolean p_flag){
        //NUM_THREADS = num_threads;
        DAMP = damping_factor;
        N = vertexCount;
        m_flag = p_flag;
    }

    @Override
    public int execute(TaskContext p_ctx) {
        BootService bootService = p_ctx.getDXRAMServiceAccessor().getService(BootService.class);
        ChunkService chunkService = p_ctx.getDXRAMServiceAccessor().getService(ChunkService.class);
        NameserviceService nameService = p_ctx.getDXRAMServiceAccessor().getService(NameserviceService.class);
        //ChunkIDRanges localChunkIDRangesIt = chunkService.cidStatus().getAllLocalChunkIDRanges(m_bootService.getNodeID());
        /**try getting all local chunks in advance**/
        Iterator<Long> localchunks = chunkService.cidStatus().getAllLocalChunkIDRanges(bootService.getNodeID()).iterator();
        localchunks.next();
        StreamSupport.stream(Spliterators.spliteratorUnknownSize(localchunks, 0).trySplit(),true).forEach(p_cid -> getIncomingPR(p_cid,p_ctx,N,DAMP));
        //StreamSupport.stream(Spliterators.spliteratorUnknownSize(localchunks, 0) ,false).forEach(p_cid -> getIncomingPR(p_cid,p_ctx,N,DAMP));

        return 0;
    }

    public void getIncomingPR(Long p_cid, TaskContext p_ctx, int vertexCount, double damping){
        ChunkService chunkService = p_ctx.getDXRAMServiceAccessor().getService(ChunkService.class);
        ChunkLocalService chunkLocalService = p_ctx.getDXRAMServiceAccessor().getService(ChunkLocalService.class);
        Vertex vertex = new Vertex(p_cid);

        chunkLocalService.getLocal().get(vertex);
        long incidenceList[] = vertex.getM_inEdges();
        Vertex[] neighbors = new Vertex[incidenceList.length];
        double tmpPR = 0.0;
        if(!m_flag){
            for (int i = 0; i < incidenceList.length; i++) {
                neighbors[i] = new Vertex(incidenceList[i]);
            }
            chunkService.get().get(neighbors);
            for(Vertex tmp : neighbors){
                tmpPR += tmp.getPR1()/(double)tmp.getOutDeg();
            }
            /*for (int i = 0; i < incidenceList.length; i++) {
                Vertex tmpChunk = new Vertex(incidenceList[i]);
                chunkService.get().get(tmpChunk);
                tmpPR += tmpChunk.getPR1()/(double)tmpChunk.getOutDeg();
            }*/
            vertex.calcPR2(N,DAMP,tmpPR);
            //System.out.println("#1 " + vertex.getPR2());
        } else {
            for (int i = 0; i < incidenceList.length; i++) {
                neighbors[i] = new Vertex(incidenceList[i]);
            }
            chunkService.get().get(neighbors);
            for(Vertex tmp : neighbors){
                tmpPR += tmp.getPR2()/(double)tmp.getOutDeg();
            }

            /*for (int i = 0; i < incidenceList.length; i++) {
                Vertex tmpChunk = new Vertex(incidenceList[i]);
                chunkService.get().get(tmpChunk);
                tmpPR += tmpChunk.getPR2()/(double)tmpChunk.getOutDeg();
            }*/
            vertex.calcPR1(N,DAMP,tmpPR);
            //System.out.println("#2 " + vertex.getPR1());
        }
        //System.out.println("# " + vertex.getM_tmpPR());
        chunkService.put().put(vertex);
    }

    @Override
    public void handleSignal(Signal p_signal) {

    }

    @Override
    public void exportObject(Exporter p_exporter) {
        //p_exporter.writeInt(NUM_THREADS);
        p_exporter.writeInt(N);
        p_exporter.writeDouble(DAMP);
        p_exporter.writeBoolean(m_flag);
    }

    @Override
    public void importObject(Importer p_importer) {
        //NUM_THREADS = p_importer.readInt(NUM_THREADS);
        N = p_importer.readInt(N);
        DAMP = p_importer.readDouble(DAMP);
        m_flag = p_importer.readBoolean(m_flag);
    }

    @Override
    public int sizeofObject() {
        return Integer.BYTES + Double.BYTES + ObjectSizeUtil.sizeofBoolean();
    }
}
