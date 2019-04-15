package de.hhu.bsinfo.dxapp.tasks;

import java.util.concurrent.atomic.DoubleAdder;
import java.util.stream.Stream;

import de.hhu.bsinfo.dxapp.chunk.LocalNonDanglingChunks;
import de.hhu.bsinfo.dxapp.chunk.Vertex;
import de.hhu.bsinfo.dxapp.chunk.VoteChunk;
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

public class RunLumpPrRoundTask implements Task {

    private int m_vertexCnt;
    private double m_damp;
    private int m_round;
    private long m_voteChunkID;

    private DoubleAdder m_PRSum;

    public RunLumpPrRoundTask(){}

    public RunLumpPrRoundTask(int p_vertexCnt, double p_damp, long p_voteChunkID, int p_round){
        m_vertexCnt = p_vertexCnt;
        m_damp = p_damp;
        m_voteChunkID = p_voteChunkID;
        m_round = p_round;
    }

    @Override
    public int execute(TaskContext taskContext) {
        BootService bootService = taskContext.getDXRAMServiceAccessor().getService(BootService.class);
        ChunkService chunkService = taskContext.getDXRAMServiceAccessor().getService(ChunkService.class);
        ChunkLocalService chunkLocalService = taskContext.getDXRAMServiceAccessor().getService(ChunkLocalService.class);
        NameserviceService nameService = taskContext.getDXRAMServiceAccessor().getService(NameserviceService.class);

        short mySlaveID = taskContext.getCtxData().getSlaveId();

        long[] localChunks = new LocalNonDanglingChunks(nameService.getChunkID(mySlaveID + "nd",333)).getLocalNonDanglingChunks();

        Vertex[] localVertices = new Vertex[localChunks.length];

        for (int i = 0; i < localVertices.length; i++) {
            localVertices[i] = new Vertex(localChunks[i]);
        }

        chunkService.get().get(localVertices);

        VoteChunk voteChunk = new VoteChunk(m_voteChunkID);
        chunkService.get().get(voteChunk);
        double danglingPR = voteChunk.getPRsum(m_round);

        Stream.of(localVertices).parallel().forEach(localVertex -> pageRankIter(localVertex,danglingPR,chunkService));

        chunkService.get().get(voteChunk, ChunkLockOperation.WRITE_LOCK_ACQ_PRE_OP);
        voteChunk.incPRsum(m_PRSum.sum(), Math.abs(m_round - 1));
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
    }

    @Override
    public void handleSignal(Signal signal) {

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
