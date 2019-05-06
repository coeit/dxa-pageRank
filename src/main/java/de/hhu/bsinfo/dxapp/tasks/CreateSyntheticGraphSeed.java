package de.hhu.bsinfo.dxapp.tasks;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;

import de.hhu.bsinfo.dxapp.chunk.Vertex;
import de.hhu.bsinfo.dxapp.chunk.VoteChunk;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.ms.MasterSlaveComputeService;
import de.hhu.bsinfo.dxram.ms.Signal;
import de.hhu.bsinfo.dxram.ms.Task;
import de.hhu.bsinfo.dxram.ms.TaskContext;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;

/**
 * Creates a synthetic Graph distributed over the Slave Peers
 */

public class CreateSyntheticGraphSeed implements Task {

    private int m_vertexCnt;
    private double m_locality;
    private int m_inDegMean;
    private int m_randomSeed;

    public CreateSyntheticGraphSeed() {}

    /**
     * @param p_vertexCnt Number of vertices in the Graph
     * @param p_locality probability of drawing an edge from a vertex (Chunk) located on the same Node
     * @param p_inDegMean Expected value of the exponential distribution from which to draw for the number of inedges
     * @param p_randomSeed Seed for the Random variable (consistency among Slaves)
     */

    public CreateSyntheticGraphSeed(int p_vertexCnt, double p_locality, int p_inDegMean, int p_randomSeed){
        m_vertexCnt = p_vertexCnt;
        m_inDegMean = p_inDegMean;
        m_locality = p_locality;
        m_randomSeed = p_randomSeed;
    }

    @Override
    public int execute(TaskContext taskContext) {
        ChunkService chunkService = taskContext.getDXRAMServiceAccessor().getService(ChunkService.class);

        short[] slaveIDs = taskContext.getCtxData().getSlaveNodeIds();
        short mySlaveIndex = taskContext.getCtxData().getSlaveId();
        short myNodeID = taskContext.getCtxData().getOwnNodeId();

        int[] slaveLocalVertexCnts = slaveLocalVertexCnts(m_vertexCnt,slaveIDs.length);

        Vertex[] vertices = new Vertex[slaveLocalVertexCnts[mySlaveIndex]];

        Random random;
        Random indgree = new Random();

        if (m_randomSeed != 0){
            random = new Random(m_randomSeed);
            indgree = new Random(m_randomSeed + 1);
        } else {
            random = new Random();
        }

        int edges = 0;

        for (int i = 0; i < slaveIDs.length; i++) {
            for (int j = 0; j < slaveLocalVertexCnts[i]; j++) {
                if(mySlaveIndex == i){
                    if (vertices[j] == null){
                        vertices[j] = new Vertex();
                    }
                }

                HashSet<Long> randCIDs = new HashSet<>();
                int k = 0;
                int indeg = getExpRandNumber(indgree);

                if(indeg >= m_vertexCnt){
                    indeg = m_vertexCnt - 1;
                }

                edges +=  indeg;

                while(k < indeg) {
                    long randCID = randCID(j + 1, m_locality, random, i, slaveIDs, slaveLocalVertexCnts);

                    if (randCIDs.add(randCID)) {
                        if (ChunkID.getCreatorID(randCID) == myNodeID) {
                            int lid = (int) ChunkID.getLocalID(randCID) - 1;

                            if (vertices[lid] == null) {
                                vertices[lid] = new Vertex();
                            }

                            vertices[lid].increment_outDeg();
                        }

                        if (mySlaveIndex == i) {
                            vertices[j].addInEdge(randCID);
                        }

                        k++;
                    }
                }
            }
        }

        chunkService.create().create(myNodeID,vertices);
        chunkService.put().put(vertices);

        /*for (int i = 0; i < vertices.length; i++) {
            System.out.print(ChunkID.toHexString(vertices[i].getID()) + " " + vertices[i].getOutDeg() + " ++ ");

            for (int j = 0; j < vertices[i].getM_inEdges().length; j++) {
                System.out.print(ChunkID.toHexString(vertices[i].getM_inEdges()[j]) + " ");
            }
            System.out.println();
        }*/

        VoteChunk vc = new VoteChunk(m_vertexCnt,edges);
        chunkService.create().create(myNodeID,vc);
        chunkService.put().put(vc);

        return 0;
    }

    private long randCID(int p_Id, double p_locality, Random p_random, int p_mySlaveID ,short[] p_slaveIDs, int[] p_slaveLocalCnts){
        short nid;
        boolean otherID = false;
        int index = p_mySlaveID;

        if(p_slaveIDs.length == 1){
            p_locality = 1;
        }

        if(p_random.nextDouble() <= p_locality){
            nid = p_slaveIDs[p_mySlaveID];
        } else {
            index = (index + p_random.nextInt(p_slaveIDs.length - 1) + 1) % p_slaveIDs.length;
            nid = p_slaveIDs[index];
            otherID = true;
        }

        long lid = p_random.nextInt(p_slaveLocalCnts[index]) + 1;

        while (lid == p_Id && !otherID){
            lid = p_random.nextInt(p_slaveLocalCnts[index]) + 1;
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

    private int[] slaveLocalVertexCnts(int p_totalVertexCnt, int p_numSlaves){
        int[] ret = new int[p_numSlaves];
        for (int i = 0; i < p_numSlaves; i++) {
            ret[i] = localVertexCnt(p_totalVertexCnt,i,p_numSlaves);
        }
        return ret;
    }

    @Override
    public void exportObject(Exporter exporter) {
        exporter.writeInt(m_vertexCnt);
        exporter.writeInt(m_inDegMean);
        exporter.writeDouble(m_locality);
        exporter.writeInt(m_randomSeed);
    }

    @Override
    public void importObject(Importer importer) {
        m_vertexCnt = importer.readInt(m_vertexCnt);
        m_inDegMean = importer.readInt(m_inDegMean);
        m_locality = importer.readDouble(m_locality);
        m_randomSeed = importer.readInt(m_randomSeed);
    }

    @Override
    public int sizeofObject() {
        return Integer.BYTES * 3 + Double.BYTES;
    }


    @Override
    public void handleSignal(Signal signal) {
    }
}
