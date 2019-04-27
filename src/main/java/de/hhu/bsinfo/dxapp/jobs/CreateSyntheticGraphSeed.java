package de.hhu.bsinfo.dxapp.jobs;

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
import de.hhu.bsinfo.dxram.job.AbstractJob;
import de.hhu.bsinfo.dxram.ms.MasterSlaveComputeService;
import de.hhu.bsinfo.dxram.ms.Signal;
import de.hhu.bsinfo.dxram.ms.Task;
import de.hhu.bsinfo.dxram.ms.TaskContext;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxutils.RandomUtils;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

public class CreateSyntheticGraphSeed extends AbstractJob {

    private int m_vertexCnt;
    private double m_locality;
    private int m_inDegMean;
    private int m_randomSeed;
    private long m_edgeCntCID;

    public CreateSyntheticGraphSeed() {}


    public CreateSyntheticGraphSeed(int p_vertexCnt, double p_locality, int p_inDegMean, long p_edgeCntCID ,int p_randomSeed){
        m_vertexCnt = p_vertexCnt;
        m_inDegMean = p_inDegMean;
        m_locality = p_locality;
        m_edgeCntCID = p_edgeCntCID;
        m_randomSeed = p_randomSeed;
    }

    @Override
    public void execute() {
        ChunkService chunkService = getService(ChunkService.class);
        MasterSlaveComputeService computeService = getService(MasterSlaveComputeService.class);

        ArrayList<Short> slaveIDs = computeService.getStatusMaster((short) 0).getConnectedSlaves();

        int[] slaveLocalVertexCnts = slaveLocalVertexCnts(m_vertexCnt,slaveIDs.size());

        Vertex[] vertices = new Vertex[m_vertexCnt];

        Random random;
        if (m_randomSeed != 0){
            random = new Random(m_randomSeed);
        } else {
            random = new Random();
        }
        int cnt = 0;
        int edges = 0;
        for (int i = 0; i < slaveIDs.size(); i++) {
            for (int j = 0; j < slaveLocalVertexCnts[i]; j++) {
                if (vertices[cnt] == null){
                    vertices[cnt] = new Vertex();
                }
                HashSet<Long> randCIDs = new HashSet<>();
                int k = 0;
                int indeg = getExpRandNumber(random);
                if(indeg >= m_vertexCnt){
                    indeg = m_vertexCnt - 1;
                }
                System.out.println("--"+indeg);

                while(k < indeg){
                    //long randCID = randCID(j + 1, m_locality, random, i, slaveIDs, slaveLocalVertexCnts);
                    //System.out.println("++"+randCID);

                    long randGID = randGID(j,random,m_locality,slaveIDs, i, slaveLocalVertexCnts);

                    /*short randNID = randNID(m_locality, random, i, slaveIDs);
                    boolean otherID = false;
                    if(getIndex(slaveIDs, randNID) != i){
                        otherID = true;
                    }
                    long randGID = randGID(j + 1,random, slaveIDs, slaveLocalVertexCnts, otherID);*/

                    if (randCIDs.add(randGID)){
                        //int globalIndex = globalIndex(randCID,slaveIDs,slaveLocalVertexCnts);
                        //long randLID = localIndex(randGID,slaveIDs,slaveLocalVertexCnts);
                        long randCID = CIDfromGID(randGID, slaveIDs, slaveLocalVertexCnts);
                        if (vertices[(int)randGID] == null){
                            vertices[(int)randGID] = new Vertex();
                        }
                        vertices[cnt].addInEdge(randCID);
                        vertices[(int)randGID].increment_outDeg();
                        k++;
                        edges++;
                    }
                }
                cnt++;
            }
        }
        cnt = 0;
        for (int i = 0; i < slaveIDs.size(); i++) {
            for (int j = 0; j < slaveLocalVertexCnts[i]; j++) {
                chunkService.create().create(slaveIDs.get(i), vertices[cnt]);
                chunkService.put().put(vertices[cnt]);
                cnt++;
            }
        }


        IntegerChunk edgeCnt = new IntegerChunk(m_edgeCntCID);
        chunkService.get().get(edgeCnt, ChunkLockOperation.WRITE_LOCK_ACQ_PRE_OP);
        edgeCnt.increment(edges);
        chunkService.put().put(edgeCnt, ChunkLockOperation.WRITE_LOCK_REL_POST_OP);



        for (int i = 0; i < vertices.length; i++) {
            System.out.print(ChunkID.toHexString(vertices[i].getID()) + " " + vertices[i].getOutDeg() + " ++ ");

            for (int j = 0; j < vertices[i].getM_inEdges().length; j++) {
                System.out.print(ChunkID.toHexString(vertices[i].getM_inEdges()[j]) + " ");
            }
            System.out.println();
        }


    }

    private long CIDfromGID(long p_gid, ArrayList<Short> p_slaveIDs, int[] p_slaveLocalCnts){
        int count = 0;
        long lid = p_gid;
        int slaveIndex = 0;
        for (int i = 0; i < p_slaveLocalCnts.length; i++) {
            count += p_slaveLocalCnts[i];
            if(p_gid >= count){
                lid = lid - p_slaveLocalCnts[i];
                slaveIndex = i;
            }
        }
        lid += 1;
        short nid = p_slaveIDs.get(slaveIndex);
        return ChunkID.getChunkID(nid,lid);
    }

    private long randGID(int p_Id, Random p_random, double p_locality, ArrayList<Short> p_slaveIDs, int p_mySlaveID, int[] p_slaveLocalCnts){

        ArrayList<Integer> otherSlaveIndx = new ArrayList<>();
        for (int i = 0; i < p_slaveIDs.size(); i++) {
            if(i != p_mySlaveID){
                otherSlaveIndx.add(i);
            }
        }
        if(p_slaveIDs.size() == 1){
            otherSlaveIndx.add(p_mySlaveID);
        }

        int[] cuts = new int[p_slaveLocalCnts.length + 1];
        cuts[0] = 0;
        for (int i = 1; i < cuts.length; i++) {
            cuts[i] = cuts[i - 1] + p_slaveLocalCnts[i - 1];
        }
        int start = cuts[p_mySlaveID];
        int end = cuts[p_mySlaveID + 1];
        int otherNodeIdx;
        boolean otherNode = false;
        long gid;
        if(p_random.nextDouble() <= p_locality){
            gid = (long) (p_random.nextDouble() * (end - start) + start);
        } else {
            otherNodeIdx = p_random.nextInt(otherSlaveIndx.size());
            if(otherNodeIdx != p_mySlaveID){
                otherNode = true;
            }
            start = cuts[otherNodeIdx];
            end = cuts[otherNodeIdx + 1];
            gid = (long) (p_random.nextDouble() * (end - start) + start);
        }
        while (localIndex(gid,p_slaveIDs,p_slaveLocalCnts) != p_Id && !otherNode){
            gid = (long) (p_random.nextDouble() * (end - start) + start);
        }
        return  gid;
    }

    private long randCID(int p_Id, double p_locality, Random p_random, int p_mySlaveID ,ArrayList<Short> p_slaveIDs, int[] p_slaveLocalCnts){

        ArrayList<Short> otherSlaveIDs = new ArrayList<>();
        for (int i = 0; i < p_slaveIDs.size(); i++) {
            if(i != p_mySlaveID){
                otherSlaveIDs.add(p_slaveIDs.get(i));
            }
        }
        if(p_slaveIDs.size() == 1){
            otherSlaveIDs.add(p_slaveIDs.get(p_mySlaveID));
        }

        short nid;
        boolean otherID = false;
        if(p_random.nextDouble() <= p_locality){
            nid = p_slaveIDs.get(p_mySlaveID);
        } else {
            nid = otherSlaveIDs.get(p_random.nextInt(otherSlaveIDs.size()));
            otherID = true;
        }

        long gid = p_random.nextInt(m_vertexCnt) + 1;
        long lid = localIndex(gid,p_slaveIDs,p_slaveLocalCnts);

        while (lid == p_Id && !otherID){
            gid = p_random.nextInt(m_vertexCnt) + 1;
            lid = localIndex(gid,p_slaveIDs,p_slaveLocalCnts);
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

    private int getIndex(ArrayList<Short> p_slaveIDs, short p_nid){
        for (int i = 0; i < p_slaveIDs.size(); i++) {
            if(p_slaveIDs.get(i) == p_nid){
                return i;
            }
        }
        return -1;
    }

    private int globalIndex(long p_cid, ArrayList<Short> p_slaveIDs, int[] p_slaveLocalCnts){
        short nid = ChunkID.getCreatorID(p_cid);
        long lid = ChunkID.getLocalID(p_cid);

        int index = getIndex(p_slaveIDs, nid);
        int count = 0;
        for (int i = 0; i < index; i++) {
            count += p_slaveLocalCnts[i];
        }
        return count + (int) lid - 1;
    }

    private long localIndex(long gid, ArrayList<Short> p_slaveIDs, int[] p_slaveLocalCnts){
        int count = 0;
        long lid = gid;
        for (int i = 0; i < p_slaveLocalCnts.length; i++) {
            count += p_slaveLocalCnts[i];
            if(gid >= count){
                lid = lid - p_slaveLocalCnts[i];
            }
        }
        return lid;

    }

    @Override
    public void exportObject(Exporter exporter) {
        super.exportObject(exporter);
        exporter.writeInt(m_vertexCnt);
        exporter.writeInt(m_inDegMean);
        exporter.writeDouble(m_locality);
        exporter.writeLong(m_edgeCntCID);
        exporter.writeInt(m_randomSeed);
    }

    @Override
    public void importObject(Importer importer) {
        super.importObject(importer);
        m_vertexCnt = importer.readInt(m_vertexCnt);
        m_inDegMean = importer.readInt(m_inDegMean);
        m_locality = importer.readDouble(m_locality);
        m_edgeCntCID = importer.readLong(m_edgeCntCID);
        m_randomSeed = importer.readInt(m_randomSeed);
    }

    @Override
    public int sizeofObject() {
        return super.sizeofObject() + Integer.BYTES * 3 + Double.BYTES + Long.BYTES;
    }
}
