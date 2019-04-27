package de.hhu.bsinfo.dxapp.tasks;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;

import de.hhu.bsinfo.dxapp.chunk.IntegerChunk;
import de.hhu.bsinfo.dxapp.chunk.Vertex;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxmem.data.ChunkLockOperation;
import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.ms.MasterSlaveComputeService;
import de.hhu.bsinfo.dxram.ms.Signal;
import de.hhu.bsinfo.dxram.ms.Task;
import de.hhu.bsinfo.dxram.ms.TaskContext;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

public class CreateSyntheticGraph implements Task {

    private int m_vertexCnt;
    private double m_locality;
    private int m_inDegMean;
    private int m_randomSeed;
    private long m_rdyCntCID;
    private long m_edgeCntCID;

    public CreateSyntheticGraph(){
        
    }
    
    public CreateSyntheticGraph(int p_vertexCnt, double p_locality, int p_inDegMean, long p_rdyCntCID, long p_edgeCntCID ,int p_randomSeed){
        m_vertexCnt = p_vertexCnt;
        m_inDegMean = p_inDegMean;
        m_locality = p_locality;
        m_rdyCntCID = p_rdyCntCID;
        m_edgeCntCID = p_edgeCntCID;
        m_randomSeed = p_randomSeed;
    }
    
    @Override
    public int execute(TaskContext taskContext) {
        ChunkService chunkService = taskContext.getDXRAMServiceAccessor().getService(ChunkService.class);
        MasterSlaveComputeService computeService = taskContext.getDXRAMServiceAccessor().getService(MasterSlaveComputeService.class);

        short mySlaveID = taskContext.getCtxData().getSlaveId();
        System.out.println("myID:" + mySlaveID);
        short[] slaveIDs = taskContext.getCtxData().getSlaveNodeIds();
        Vertex[] localVertices = new Vertex[localVertexCnt(m_vertexCnt,mySlaveID,slaveIDs.length)];
        HashMap<Long, Integer> remoteInEdges = new HashMap<>();

        Random random;
        if (m_randomSeed != 0){
            random = new Random(m_randomSeed);
        } else {
            random = new Random();
        }

        int edges = 0;

        for (int i = 0; i < localVertices.length; i++) {
            if (localVertices[i] == null){
                localVertices[i] = new Vertex();
            }
            HashSet<Long> randIDs = new HashSet<>();
            int j = 0;
            int indeg = getExpRandNumber(random);
            while(j < indeg){
                long randCID = randCID(i + 1,m_locality,localVertices.length,random,mySlaveID,slaveIDs);
                int randLID = (int)ChunkID.getLocalID(randCID) - 1;
                if(randIDs.add(randCID)){
                    if (localVertices[randLID] == null){
                        localVertices[randLID] = new Vertex();
                    }
                    localVertices[i].addInEdge(randCID);
                    if(ChunkID.getCreatorID(randCID) == taskContext.getCtxData().getOwnNodeId()){
                        localVertices[randLID].increment_outDeg();
                    } else {
                        remoteInEdges.putIfAbsent(randCID, 0);
                        remoteInEdges.put(randCID, remoteInEdges.get(randCID) + 1);
                    }
                    j++;
                    edges++;
                }
            }
            localVertices[i].invokeVertexPR(m_vertexCnt);
        }
        chunkService.create().create(taskContext.getCtxData().getOwnNodeId(),localVertices);
        chunkService.put().put(localVertices);

        System.out.println("Chunk Creation Done...");

        IntegerChunk rdyCnt = new IntegerChunk(m_rdyCntCID);
        chunkService.get().get(rdyCnt, ChunkLockOperation.WRITE_LOCK_ACQ_PRE_OP);
        rdyCnt.increment();
        chunkService.put().put(rdyCnt, ChunkLockOperation.WRITE_LOCK_REL_POST_OP);

        while(rdyCnt.get_value() != slaveIDs.length){
            try {
                Thread.sleep(100);
            } catch (final InterruptedException ignore) {

            }
            chunkService.get().get(rdyCnt);
        }
        int cnt = 0;
        for (long remoteInEdge : remoteInEdges.keySet()){
            Vertex remoteVertex = new Vertex(remoteInEdge);
            chunkService.get().get(remoteVertex);
            remoteVertex.increment_outDeg(remoteInEdges.get(remoteInEdge));
            chunkService.put().put(remoteVertex);
            cnt++;
            if(cnt % 1000000 == 0){
                System.out.print(".");
            }
        }
        System.out.println("\nOutDegrees added...");
        IntegerChunk edgeCnt = new IntegerChunk(m_edgeCntCID);
        chunkService.get().get(edgeCnt, ChunkLockOperation.WRITE_LOCK_ACQ_PRE_OP);
        edgeCnt.increment(edges);
        chunkService.put().put(edgeCnt, ChunkLockOperation.WRITE_LOCK_REL_POST_OP);



        /*for (int i = 0; i < localVertices.length; i++) {
            System.out.print(ChunkID.toHexString(localVertices[i].getID()) + " " + localVertices[i].getOutDeg() + " ++ ");

            for (int j = 0; j < localVertices[i].getM_inEdges().length; j++) {
                System.out.print(ChunkID.toHexString(localVertices[i].getM_inEdges()[j]) + " ");
            }
            System.out.println();
        }*/



        return 0;
    }

    private long randCID(int p_Id, double p_locality, int p_localVertexCnt, Random p_random, short p_mySlaveID ,short[] p_slaveIDs){

        ArrayList<Short> otherSlaveIDs = new ArrayList<>();
        for (int i = 0; i < p_slaveIDs.length; i++) {
            if(i != p_mySlaveID){
                otherSlaveIDs.add(p_slaveIDs[i]);
            }
        }
        if(p_slaveIDs.length == 1){
            otherSlaveIDs.add(p_slaveIDs[p_mySlaveID]);
        }

        long lid = p_random.nextInt(p_localVertexCnt) + 1;
        short nid;

        if(p_random.nextDouble() <= p_locality){
            nid = p_slaveIDs[p_mySlaveID];
            while(lid == p_Id){
                lid = p_random.nextInt(p_localVertexCnt) + 1;
            }
        } else {
            nid = otherSlaveIDs.get(p_random.nextInt(otherSlaveIDs.size()));
        }
        return ChunkID.getChunkID(nid, lid);
    }

    private int getExpRandNumber(Random p_random){
        return (int) (Math.log(1 - p_random.nextDouble())/(- Math.pow(m_inDegMean,-1)));
    }

    private int localVertexCnt(int p_totalVertexCnt, int p_slaveID, int p_numSlaves){
        int mod = p_totalVertexCnt % p_numSlaves;
        double div = (double)p_totalVertexCnt/(double)p_numSlaves;
        if(p_slaveID < mod){
            return (int) Math.ceil(div);
        }
        return (int) Math.floor(div);
    }

    @Override
    public void handleSignal(Signal signal) {

    }

    @Override
    public void exportObject(Exporter exporter) {
        exporter.writeInt(m_vertexCnt);
        exporter.writeInt(m_inDegMean);
        exporter.writeDouble(m_locality);
        exporter.writeLong(m_rdyCntCID);
        exporter.writeLong(m_edgeCntCID);
        exporter.writeInt(m_randomSeed);
    }

    @Override
    public void importObject(Importer importer) {
        m_vertexCnt = importer.readInt(m_vertexCnt);
        m_inDegMean = importer.readInt(m_inDegMean);
        m_locality = importer.readDouble(m_locality);
        m_rdyCntCID = importer.readLong(m_rdyCntCID);
        m_edgeCntCID = importer.readLong(m_edgeCntCID);
        m_randomSeed = importer.readInt(m_randomSeed);
    }

    @Override
    public int sizeofObject() {
        return Integer.BYTES * 3 + Double.BYTES + Long.BYTES * 2;
    }
}
