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
import java.util.concurrent.atomic.DoubleAccumulator;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class RunPrRoundTask implements Task {

    //private int NUM_THREADS;
    private int N;
    private double m_damp;
    private int m_round;
    //private DoubleAdder m_PRsum = new DoubleAdder();
    private DoubleAdder m_PRerr = new DoubleAdder();
    private long m_voteChunkID;

    public RunPrRoundTask(){}

    public RunPrRoundTask(int vertexCount, double p_damp, int p_round, long p_voteChunkID){
        //NUM_THREADS = num_threads;
        m_damp = p_damp;
        N = vertexCount;
        m_round = p_round;
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
        StreamSupport.stream(Spliterators.spliteratorUnknownSize(localchunks, 0).trySplit(),true).forEach(p_cid -> getIncomingPR(p_cid,p_ctx,N,m_damp));*/
        //StreamSupport.stream(Spliterators.spliteratorUnknownSize(localchunks, 0) ,false).forEach(p_cid -> getIncomingPR(p_cid,p_ctx,N,m_damp));


        //final AtomicInteger voteCnt = new AtomicInteger(0);
        Iterator<Long> localchunks = chunkService.cidStatus().getAllLocalChunkIDRanges(bootService.getNodeID()).iterator();
        //Spliterator<Long> localchunks = chunkService.cidStatus().getAllLocalChunkIDRanges(bootService.getNodeID()).spliterator();
        localchunks.next();

        Vertex[] localVertices = new Vertex[(int)chunkService.status().getStatus(bootService.getNodeID()).getLIDStoreStatus().getCurrentLIDCounter()];
        for (int i = 0; i < localVertices.length; i++) {
            localVertices[i] = new Vertex(localchunks.next());
        }

        //chunkService.get().get(localVertices);
        chunkLocalService.getLocal().get(localVertices);

        Stream.of(localVertices).parallel().forEach(localVertex -> getIncomingPR(localVertex,chunkService));
        //System.out.println(System.currentTimeMillis());

        VoteChunk voteChunk = new VoteChunk(m_voteChunkID);
        chunkService.get().get(voteChunk,ChunkLockOperation.WRITE_LOCK_ACQ_PRE_OP);
        //voteChunk.incPRsum(m_PRsum.sum());
        voteChunk.incPRerr(m_PRerr.sum());
        chunkService.put().put(voteChunk, ChunkLockOperation.WRITE_LOCK_REL_POST_OP);
        //System.out.println("RunPrErr: " + m_PRerr.sum() + " RoundPRsum: " + m_PRsum.sum());

        return 0;
    }

    public void getIncomingPR(Vertex p_vertex, ChunkService p_chunkService){

        long incidenceList[] = p_vertex.getM_inEdges();
        Vertex[] neighbors = new Vertex[incidenceList.length];
        double tmpPR = 0.0;

        for (int i = 0; i < incidenceList.length; i++) {
            neighbors[i] = new Vertex(incidenceList[i]);
        }

        p_chunkService.get().get(neighbors);
        for(Vertex tmp : neighbors){
            tmpPR += tmp.getPageRank(m_round)/(double)tmp.getOutDeg();
        }
        p_vertex.calcPageRank(N,m_damp,tmpPR, Math.abs(m_round - 1));


        //m_PRsum.add(p_vertex.getPageRank(Math.abs(m_round -1)));
        m_PRerr.add(Math.abs(p_vertex.getPageRank(Math.abs(m_round - 1)) - p_vertex.getPageRank(m_round)));
        //System.out.println(p_vertex.getPageRank(Math.abs(m_round - 1)) + "\t" + p_vertex.getPageRank(m_round));

        p_chunkService.put().put(p_vertex);
        //double err = p_vertex.getPageRank(Math.abs(m_round - 1)) - p_vertex.getPageRank(m_round);
        //System.out.println(p_vertex.get_name() + " " + ChunkID.toHexString(p_vertex.getID()) + ": " + p_vertex.getPageRank(Math.abs(m_round - 1)) + " " + p_vertex.getPageRank(m_round));


        /*if(!m_flag){
            for (int i = 0; i < incidenceList.length; i++) {
                neighbors[i] = new Vertex(incidenceList[i]);
            }

            p_chunkService.get().get(neighbors);
            for(Vertex tmp : neighbors){
                tmpPR += tmp.getPR1()/(double)tmp.getOutDeg();
            }
            p_vertex.calcPR2(N,m_damp,tmpPR);
            m_PRsum += p_vertex.getPR2();

            double err = p_vertex.getPR2() - p_vertex.getPR1();
            if(Math.abs(err) < 0.00001){ ret = 1;}
            System.out.println(p_vertex.get_name() + " " + ChunkID.toHexString(p_vertex.getID()) + ": " + p_vertex.getPR2() + " " + p_vertex.getPR1());

        } else {
            for (int i = 0; i < incidenceList.length; i++) {
                neighbors[i] = new Vertex(incidenceList[i]);
            }

            p_chunkService.get().get(neighbors);

            for(Vertex tmp : neighbors){
                tmpPR += tmp.getPR2()/(double)tmp.getOutDeg();
            }

            p_vertex.calcPR1(N,m_damp,tmpPR);
            m_PRsum += p_vertex.getPR1();

            double err = p_vertex.getPR1() - p_vertex.getPR2();
            if(Math.abs(err) < 0.00001){ ret = 1;}
            System.out.println(p_vertex.get_name() + " " + ChunkID.toHexString(p_vertex.getID()) + ": " + p_vertex.getPR1() + " " + p_vertex.getPR2());
        }*/

    }

    @Override
    public void handleSignal(Signal p_signal) {

    }

    @Override
    public void exportObject(Exporter p_exporter) {
        //p_exporter.writeInt(NUM_THREADS);
        p_exporter.writeInt(N);
        p_exporter.writeDouble(m_damp);
        p_exporter.writeInt(m_round);
        p_exporter.writeLong(m_voteChunkID);
    }

    @Override
    public void importObject(Importer p_importer) {
        //NUM_THREADS = p_importer.readInt(NUM_THREADS);
        N = p_importer.readInt(N);
        m_damp = p_importer.readDouble(m_damp);
        m_round = p_importer.readInt(m_round);
        m_voteChunkID = p_importer.readLong(m_voteChunkID);
    }

    @Override
    public int sizeofObject() {
        return Integer.BYTES * 2 + Double.BYTES + Long.BYTES;
    }
}
