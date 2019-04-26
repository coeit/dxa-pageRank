package de.hhu.bsinfo.dxapp.tasks;

import java.util.Iterator;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.stream.Stream;

import de.hhu.bsinfo.dxapp.chunk.LocalDanglingChunks;
import de.hhu.bsinfo.dxapp.chunk.LocalNonDanglingChunks;
import de.hhu.bsinfo.dxapp.chunk.Vertex;
import de.hhu.bsinfo.dxapp.chunk.VoteChunk;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxmem.data.ChunkLockOperation;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.ms.Signal;
import de.hhu.bsinfo.dxram.ms.Task;
import de.hhu.bsinfo.dxram.ms.TaskContext;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

public class RunLumpPrRoundTask implements Task {

    private int m_vertexCnt;
    private double m_damp;
    private int m_round;
    private long m_voteChunkID;
    private boolean m_calcDanglingPR;

    private DoubleAdder m_PRErr = new DoubleAdder();
    private DoubleAdder m_PRSum = new DoubleAdder();

    public RunLumpPrRoundTask(){}

    public RunLumpPrRoundTask(int p_vertexCnt, double p_damp, long p_voteChunkID, int p_round, boolean p_calcDanglingPR){
        m_vertexCnt = p_vertexCnt;
        m_damp = p_damp;
        m_voteChunkID = p_voteChunkID;
        m_round = p_round;
        m_calcDanglingPR = p_calcDanglingPR;
    }

    @Override
    public int execute(TaskContext taskContext) {
        BootService bootService = taskContext.getDXRAMServiceAccessor().getService(BootService.class);
        ChunkService chunkService = taskContext.getDXRAMServiceAccessor().getService(ChunkService.class);
        ChunkLocalService chunkLocalService = taskContext.getDXRAMServiceAccessor().getService(ChunkLocalService.class);
        NameserviceService nameService = taskContext.getDXRAMServiceAccessor().getService(NameserviceService.class);

        short mySlaveID = taskContext.getCtxData().getSlaveId();
        long[] localChunks;
        /*if(!m_calcDanglingPR){
            LocalNonDanglingChunks localNonDanglingChunks = new LocalNonDanglingChunks(nameService.getChunkID(mySlaveID + "nd",333));
            chunkService.get().get(localNonDanglingChunks);
            localChunks = localNonDanglingChunks.getLocalNonDanglingChunks();
        } else {
            LocalDanglingChunks localDanglingChunks = new LocalDanglingChunks(nameService.getChunkID(mySlaveID + "d",333));
            chunkService.get().get(localDanglingChunks);
            localChunks = localDanglingChunks.getLocalDanglingChunks();
        }

        if(localChunks.length == 0){
            return 0;
        }

        Vertex[] localVertices = new Vertex[localChunks.length];

        for (int i = 0; i < localVertices.length; i++) {
            localVertices[i] = new Vertex(localChunks[i]);
        }*/

        Iterator<Long> localchunks = chunkService.cidStatus().getAllLocalChunkIDRanges(bootService.getNodeID()).iterator();
        localchunks.next();

        Vertex[] localVertices = new Vertex[(int)chunkService.status().getStatus(bootService.getNodeID()).getLIDStoreStatus().getCurrentLIDCounter()];
        for (int i = 0; i < localVertices.length; i++) {
            localVertices[i] = new Vertex(localchunks.next());
        }

        chunkService.get().get(localVertices);

        VoteChunk voteChunk = new VoteChunk(m_voteChunkID);
        chunkService.get().get(voteChunk);
        double danglingPR = voteChunk.getPRsum(m_round);

        if(!m_calcDanglingPR){
            Stream.of(localVertices).parallel().forEach(localVertex -> {
                if(localVertex.getOutDeg() != 0){
                    pageRankIter(localVertex,danglingPR,chunkService);
                }
            });
        } else {
            Stream.of(localVertices).parallel().forEach(localVertex -> {
                if(localVertex.getOutDeg() == 0){
                    pageRankIter(localVertex,danglingPR,chunkService);
                }
            });
        }


        //System.out.println("danglingPR:" + danglingPR);
        //System.out.println("sum: " + m_PRSum.sum());

        chunkService.get().get(voteChunk, ChunkLockOperation.WRITE_LOCK_ACQ_PRE_OP);
        voteChunk.incPRsum(m_PRSum.sum(), Math.abs(m_round - 1));
        voteChunk.incPRerr(m_PRErr.sum());
        chunkService.put().put(voteChunk, ChunkLockOperation.WRITE_LOCK_REL_POST_OP);

        return 0;
    }

    public void pageRankIter(Vertex p_vertex, double p_danglingPR, ChunkService p_chunkService){
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
        p_vertex.calcLumpPageRank(m_vertexCnt, m_damp, tmpPR, p_danglingPR ,Math.abs(m_round - 1));

        m_PRSum.add(p_vertex.getPageRank(Math.abs(m_round - 1)));
        m_PRErr.add(Math.abs(p_vertex.getPageRank(Math.abs(m_round - 1)) - p_vertex.getPageRank(m_round)));

        if(m_calcDanglingPR){
            p_vertex.setPageRank(m_round);
        }

        System.out.println(p_vertex.get_name() + " " + ChunkID.toHexString(p_vertex.getID()) + ": " + p_vertex.getPageRank(Math.abs(m_round - 1)) + " " + p_vertex.getPageRank(m_round));

        p_chunkService.put().put(p_vertex);

    }

    @Override
    public void handleSignal(Signal signal) {

    }

    @Override
    public void exportObject(Exporter p_exporter) {
        p_exporter.writeInt(m_vertexCnt);
        p_exporter.writeDouble(m_damp);
        p_exporter.writeInt(m_round);
        p_exporter.writeLong(m_voteChunkID);
        p_exporter.writeBoolean(m_calcDanglingPR);
    }

    @Override
    public void importObject(Importer p_importer) {
        m_vertexCnt = p_importer.readInt(m_vertexCnt);
        m_damp = p_importer.readDouble(m_damp);
        m_round = p_importer.readInt(m_round);
        m_voteChunkID = p_importer.readLong(m_voteChunkID);
        m_calcDanglingPR = p_importer.readBoolean(m_calcDanglingPR);
    }

    @Override
    public int sizeofObject() {
        return Integer.BYTES * 2 + Double.BYTES + Long.BYTES + ObjectSizeUtil.sizeofBoolean();
    }
}
