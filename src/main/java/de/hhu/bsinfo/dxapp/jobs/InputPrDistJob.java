/*package de.hhu.bsinfo.dxapp.jobs;

import de.hhu.bsinfo.dxapp.chunk.PageRankInVertex;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.job.AbstractJob;
import de.hhu.bsinfo.dxram.ms.MasterSlaveComputeService;
import de.hhu.bsinfo.dxutils.NodeID;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class InputPrDistJob extends AbstractJob {

    private String m_filename;

    public InputPrDistJob() {
    }

    public InputPrDistJob(String p_filename) {
        m_filename = p_filename;
    }

    @Override
    public void execute() {
        ChunkLocalService chunkLocalService = getService(ChunkLocalService.class);
        MasterSlaveComputeService computeService = getService(MasterSlaveComputeService.class);
        BootService bootService = getService(BootService.class);

        //int vertexCnt = 0;
        HashMap<Integer, PageRankInVertex> chunkMap = new HashMap<>();
        HashMap<Integer, Integer> outdeg = new HashMap<>();
        System.out.println("Node " + NodeID.toHexString(bootService.getNodeID()) + " reading " + m_filename);
        try(BufferedReader br = new BufferedReader(new FileReader(m_filename))){
            String line;
            Integer v1, v2;
            while ((line = br.readLine()) != null){
                String[] split = line.split(" ");
                v1 = Integer.parseInt(split[0]); //ERROR CHECK
                v2 = Integer.parseInt(split[1]);

                if(!outdeg.containsKey(v1)){
                    outdeg.put(v1, 0);
                }
                if(!chunkMap.containsKey(v2)){
                    PageRankInVertex tmp = new PageRankInVertex(v2);
                    chunkMap.put(v2, tmp);
                    //vertexCnt++;
                }
                //increase outdeg with writelockpreop
                chunkMap.get(v2).addInEdge(v1);
                outdeg.put(v1,outdeg.get(v1) + 1);
            }
        }  catch (IOException e) {
            e.printStackTrace();
        }

        for (Integer vertexPR : chunkMap.keySet()) {
            //chunkMap.get(vertexPR).invokeVertexPR(chunkMap.keySet().size());
            System.out.println(vertexPR);
            chunkMap.get(vertexPR).invokeVertexPR(chunkCnt);
            //m_chunkService.create().create(connectedSlaves.get(slaveIndex % 2),chunkMap.get(vertexPR));
            m_chunkService.create().create(m_bootService.getNodeID(),chunkMap.get(vertexPR));
            System.out.println(ChunkID.toHexString(chunkMap.get(vertexPR).getID()));
            m_nameService.register(chunkMap.get(vertexPR), vertexPR.toString());
            slaveIndex++;
            m_chunkService.put().put(chunkMap.get(vertexPR));

        }
        Long out;
        for (Integer in : outdeg.keySet()){
            while(true) {
                out = m_nameService.getChunkID(Integer.toString(in), 10);
                if(out != -1){
                    PageRankInVertex out_v = new PageRankInVertex(out);
                    m_chunkService.get().get(out_v, ChunkLockOperation.WRITE_LOCK_ACQ_PRE_OP);
                    out_v.increment_outDeg(outdeg.get(in));
                    m_chunkService.put().put(out_v,ChunkLockOperation.WRITE_LOCK_REL_POST_OP);
                    break;
                } else {
                    try {
                        Thread.sleep(10);
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

    @Override
    public void importObject(Importer p_importer) {
        super.importObject(p_importer);
    }

    @Override
    public void exportObject(Exporter p_exporter) {
        super.exportObject(p_exporter);
    }

    @Override
    public int sizeofObject() {
        return super.sizeofObject();
    }
}
*/
