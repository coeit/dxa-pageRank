package de.hhu.bsinfo.dxapp.jobs;

import de.hhu.bsinfo.dxapp.chunk.PageRankInVertex;
import de.hhu.bsinfo.dxapp.chunk.Vertex;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxmem.data.ChunkIDRanges;
import de.hhu.bsinfo.dxmem.data.ChunkLockOperation;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.job.AbstractJob;
import de.hhu.bsinfo.dxram.ms.MasterSlaveComputeService;
import de.hhu.bsinfo.dxutils.NodeID;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;

public class InputPrDistJob extends AbstractJob {

    private String m_filename;
    private int m_vertexCnt;

    public InputPrDistJob() {
    }

    public InputPrDistJob(String p_filename, int p_vertexCnt) {
        m_filename = p_filename;
        m_vertexCnt = p_vertexCnt;
    }

    @Override
    public void execute() {
        ChunkLocalService chunkLocalService = getService(ChunkLocalService.class);
        ChunkService chunkService = getService(ChunkService.class);
        MasterSlaveComputeService computeService = getService(MasterSlaveComputeService.class);
        BootService bootService = getService(BootService.class);

        ArrayList<Short> slaveIDs = computeService.getStatusMaster((short) 0).getConnectedSlaves();
        /*int vertexCntToRead = 0;
        try {
             vertexCntToRead = (int) Math.ceil(lastInputNumber()/ (double) slaveCnt);
        } catch (IOException e) {
            e.printStackTrace();
        }
        Vertex[] vertices = new Vertex[vertexCntToRead];*/
        HashMap<Integer, Vertex> chunkMap = new HashMap<>();
        HashMap<Long, Integer> outdeg = new HashMap<>();
        System.out.println("Node " + NodeID.toHexString(bootService.getNodeID()) + " reading " + m_filename);
        try(BufferedReader br = new BufferedReader(new FileReader(m_filename))){
            String line;
            Integer v1, v2;
            long v1ChunkID;
            while ((line = br.readLine()) != null){
                String[] split = line.split(" ");
                v1 = Integer.parseInt(split[0]); //ERROR CHECK
                v2 = Integer.parseInt(split[1]);
                v1ChunkID = correspondingChunkID(v1,slaveIDs);

                if(!outdeg.containsKey(v1ChunkID)){
                    outdeg.put(v1ChunkID, 0);
                }
                if(!chunkMap.containsKey(v2)){
                    Vertex tmp = new Vertex(v2);
                    chunkMap.put(v2, tmp);
                    //vertexCnt++;
                }
                //increase outdeg with writelockpreop
                chunkMap.get(v2).addInEdge(v1ChunkID);
                outdeg.put(v1ChunkID,outdeg.get(v1ChunkID) + 1);
            }
        }  catch (IOException e) {
            e.printStackTrace();
        }

        for (Integer vertexPR : chunkMap.keySet()) {
            chunkMap.get(vertexPR).invokeVertexPR(m_vertexCnt);
            chunkLocalService.createLocal().create(chunkMap.get(vertexPR));
            chunkService.put().put(chunkMap.get(vertexPR));

        }

        for (long in : outdeg.keySet()){
            Vertex vertex = new Vertex(in);
            ChunkIDRanges ranges = chunkService.cidStatus().getAllLocalChunkIDRanges(ChunkID.getCreatorID(in));
            while(true) {
                if(ranges.isInRange(in)){
                    System.out.println(ChunkID.toHexString(in));
                    chunkService.get().get(vertex,ChunkLockOperation.WRITE_LOCK_ACQ_PRE_OP);
                    //vertex.increment_outDeg(outdeg.get(in));
                    chunkService.put().put(vertex,ChunkLockOperation.WRITE_LOCK_REL_POST_OP);
                    break;
                } else {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    public long correspondingChunkID(int p_vertex, ArrayList<Short> slaveIDs){
        int slaveCnt = slaveIDs.size();
        short nid = slaveIDs.get((short) ((p_vertex-1) % slaveCnt));
        long lid = (long) (((p_vertex-1) / slaveCnt) + 1);
        return ChunkID.getChunkID(nid,lid);
    }

    public long lastInputNumber() throws IOException{
        File file = new File(m_filename);
        RandomAccessFile raf = new RandomAccessFile(file, "r");
        long l = Long.MAX_VALUE;
        int n = String.valueOf(l).length();
        byte[] bytes = new byte[n];
        raf.seek(file.length() - n);
        raf.read(bytes, 0, n);
        String s = new String(bytes);
        String[] split = s.split(" ");
        long last = Long.parseLong(split[split.length-1].substring(0, split[split.length-1].length() - 1));
        return last;
    }

    @Override
    public void importObject(Importer p_importer) {
        super.importObject(p_importer);
        m_filename = p_importer.readString(m_filename);
        m_vertexCnt = p_importer.readInt(m_vertexCnt);
    }

    @Override
    public void exportObject(Exporter p_exporter) {
        super.exportObject(p_exporter);
        p_exporter.writeString(m_filename);
        p_exporter.writeInt(m_vertexCnt);
    }

    @Override
    public int sizeofObject() {
        return super.sizeofObject() + ObjectSizeUtil.sizeofString(m_filename) + Integer.BYTES;
    }
}
