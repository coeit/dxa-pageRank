package de.hhu.bsinfo.dxapp.tasks;

import de.hhu.bsinfo.dxapp.chunk.Vertex;
import de.hhu.bsinfo.dxapp.chunk.VoteChunk;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class RunPrRoundTask implements Task {

    //private int NUM_THREADS;
    private int N;
    private double DAMP;
    private boolean m_flag;
    private double m_PRsum;
    private long m_voteChunkID;

    public RunPrRoundTask(){}

    public RunPrRoundTask(int vertexCount, double damping_factor, boolean p_flag, long p_voteChunkID){
        //NUM_THREADS = num_threads;
        DAMP = damping_factor;
        N = vertexCount;
        m_flag = p_flag;
        m_voteChunkID = p_voteChunkID;
    }

    @Override
    public int execute(TaskContext p_ctx) {
        BootService bootService = p_ctx.getDXRAMServiceAccessor().getService(BootService.class);
        ChunkService chunkService = p_ctx.getDXRAMServiceAccessor().getService(ChunkService.class);
        ChunkLocalService chunkLocalService = p_ctx.getDXRAMServiceAccessor().getService(ChunkLocalService.class);
        NameserviceService nameService = p_ctx.getDXRAMServiceAccessor().getService(NameserviceService.class);
        //ChunkIDRanges localChunkIDRangesIt = chunkService.cidStatus().getAllLocalChunkIDRanges(m_bootService.getNodeID());
        /**try getting all local chunks in advance**/
        /*Iterator<Long> localchunks = chunkService.cidStatus().getAllLocalChunkIDRanges(bootService.getNodeID()).iterator();
        localchunks.next();
        StreamSupport.stream(Spliterators.spliteratorUnknownSize(localchunks, 0).trySplit(),true).forEach(p_cid -> getIncomingPR(p_cid,p_ctx,N,DAMP));*/
        //StreamSupport.stream(Spliterators.spliteratorUnknownSize(localchunks, 0) ,false).forEach(p_cid -> getIncomingPR(p_cid,p_ctx,N,DAMP));


        m_PRsum = 0.0;
        final AtomicInteger voteCnt = new AtomicInteger(0);
        Iterator<Long> localchunks = chunkService.cidStatus().getAllLocalChunkIDRanges(bootService.getNodeID()).iterator();
        //Spliterator<Long> localchunks = chunkService.cidStatus().getAllLocalChunkIDRanges(bootService.getNodeID()).spliterator();
        localchunks.next();

        Vertex[] localVertices = new Vertex[(int)chunkService.status().getStatus(bootService.getNodeID()).getLIDStoreStatus().getCurrentLIDCounter()];
        for (int i = 0; i < localVertices.length; i++) {
            localVertices[i] = new Vertex(localchunks.next());
        }

        chunkService.get().get(localVertices);
        //chunkLocalService.getLocal().get(localVertices);

        Stream.of(localVertices).parallel().forEach(localVertex -> voteCnt.getAndAdd(getIncomingPR(localVertex,p_ctx)));

        VoteChunk voteChunk = new VoteChunk(m_voteChunkID);
        chunkService.get().get(voteChunk,ChunkLockOperation.WRITE_LOCK_ACQ_PRE_OP);
        voteChunk.incVotes(voteCnt.get());
        voteChunk.incPrSum(m_PRsum);
        chunkService.put().put(voteChunk, ChunkLockOperation.WRITE_LOCK_REL_POST_OP);
        System.out.println("RunPr Vote Cnt: " + voteCnt.get());

        return 0;
    }

    public int getIncomingPR(Vertex p_vertex, TaskContext p_ctx){
        ChunkService chunkService = p_ctx.getDXRAMServiceAccessor().getService(ChunkService.class);
        /*ChunkLocalService chunkLocalService = p_ctx.getDXRAMServiceAccessor().getService(ChunkLocalService.class);
        Vertex vertex = new Vertex(p_cid);

        chunkLocalService.getLocal().get(vertex);*/
        int ret = 0;
        long incidenceList[] = p_vertex.getM_inEdges();
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
            p_vertex.calcPR2(N,DAMP,tmpPR);
            m_PRsum += p_vertex.getPR2();

            double err = p_vertex.getPR2() - p_vertex.getPR1();
            if(Math.abs(err) < 0.000001){ ret = 1;}
            System.out.println(p_vertex.get_name() + " " + ChunkID.toHexString(p_vertex.getID()) + ": " + p_vertex.getPR2() + " " + p_vertex.getPR1());

        } else {
            for (int i = 0; i < incidenceList.length; i++) {
                neighbors[i] = new Vertex(incidenceList[i]);
            }

            chunkService.get().get(neighbors);

            for(Vertex tmp : neighbors){
                tmpPR += tmp.getPR2()/(double)tmp.getOutDeg();
            }

            p_vertex.calcPR1(N,DAMP,tmpPR);
            m_PRsum += p_vertex.getPR1();

            double err = p_vertex.getPR1() - p_vertex.getPR2();
            if(Math.abs(err) < 0.000001){ ret = 1;}
            System.out.println(p_vertex.get_name() + " " + ChunkID.toHexString(p_vertex.getID()) + ": " + p_vertex.getPR1() + " " + p_vertex.getPR2());
        }
        chunkService.put().put(p_vertex);

        return ret;
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
        p_exporter.writeLong(m_voteChunkID);
    }

    @Override
    public void importObject(Importer p_importer) {
        //NUM_THREADS = p_importer.readInt(NUM_THREADS);
        N = p_importer.readInt(N);
        DAMP = p_importer.readDouble(DAMP);
        m_flag = p_importer.readBoolean(m_flag);
        m_voteChunkID = p_importer.readLong(m_voteChunkID);
    }

    @Override
    public int sizeofObject() {
        return Integer.BYTES + Double.BYTES + Long.BYTES +ObjectSizeUtil.sizeofBoolean();
    }
}
