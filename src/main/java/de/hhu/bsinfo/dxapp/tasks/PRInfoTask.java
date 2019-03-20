package de.hhu.bsinfo.dxapp.tasks;

import de.hhu.bsinfo.dxapp.chunk.Vertex;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxapp.chunk.PageRankInVertex;
import de.hhu.bsinfo.dxram.ms.MasterSlaveComputeService;
import de.hhu.bsinfo.dxram.ms.Signal;
import de.hhu.bsinfo.dxram.ms.Task;
import de.hhu.bsinfo.dxram.ms.TaskContext;
import de.hhu.bsinfo.dxram.nameservice.NameserviceEntryStr;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxutils.NodeID;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.StreamSupport;

import java.math.BigDecimal;

public class PRInfoTask implements Task {

    String m_outDir;

    public PRInfoTask(){
    }

    public PRInfoTask(String p_outDir){

    }

    @Override
    public int execute(TaskContext p_ctx) {
        ChunkService chunkService = p_ctx.getDXRAMServiceAccessor().getService(ChunkService.class);
        BootService bootService = p_ctx.getDXRAMServiceAccessor().getService(BootService.class);

        Vertex[] vertex = new Vertex[chunkService.cidStatus().getAllLocalChunkIDRanges(bootService.getNodeID()).size()];
        System.out.println("getAllLocalChunkIDRanges.size: " + chunkService.cidStatus().getAllLocalChunkIDRanges(bootService.getNodeID()).size());
        System.out.println("getTotalLIDsInStore: " + chunkService.status().getStatus(bootService.getNodeID()).getLIDStoreStatus().getTotalLIDsInStore());
        /*for (int i = 0; i < vertex.length; i++) {
            vertex[i] = new Vertex(incidenceList[i]);
        }*/

        Iterator<Long> localchunks = chunkService.cidStatus().getAllLocalChunkIDRanges(bootService.getNodeID()).iterator();
        //Spliterator<Long> localchunks = chunkService.cidStatus().getAllLocalChunkIDRanges(bootService.getNodeID()).spliterator();
        localchunks.next();
        StreamSupport.stream(Spliterators.spliteratorUnknownSize(localchunks, 0),false).forEach(p_cid -> printInfo(p_cid,p_ctx));

        /*try (Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("filename.txt"), "utf-8"))) {
        } catch (IOException e){
            e.printStackTrace();
        }*/



        /*for (short nid : m_bootService.getOnlinePeerNodeIDs()){
            String n_id = NodeID.toHexString(nid).substring(2,6);
            nids.add(n_id);
        }

        ArrayList<Short> slaveIDs = m_computeService.getStatusMaster((short) 0).getConnectedSlaves();


        for (short slaveID : slaveIDs){
            Iterator<Long> localChunks = m_chunkService.cidStatus().getAllLocalChunkIDRanges(slaveID).iterator();
            //if (!vertexPR.getName().equals(NodeID.toHexString(m_bootService.getNodeID()).substring(2,6))) {
            //if(!nids.contains(vertexPR.getName())){
            localChunks.next();
            while(localChunks.hasNext()){
                //PageRankInVertex vert = new PageRankInVertex(m_nameService.getChunkID(vertexPR.getName(),100));
                PageRankInVertex vert = new PageRankInVertex(localChunks.next());
                m_chunkService.get().get(vert);
		        System.out.print(vert.get_name() + ": ");
                //System.out.print(vertexPR.getName() + ": " + vert.getM_currPR());
                //System.out.print(vertexPR.getName() + ": " + ChunkID.toHexString(m_nameService.getChunkID(vertexPR.getName(),100)) + ": ");
                for (Long i : vert.getM_inEdges()){
                    System.out.print(ChunkID.toHexString(i) + " ");
                }
                System.out.println("\noutdeg: " + vert.getM_outDeg());// + " " + vert.get_indegAlt());
                System.out.println("PR: " + vert.getM_currPR());
                System.out.println("=====");
            }

        }*/
        return 0;
    }

    public void printInfo(Long p_cid, TaskContext p_ctx){
        ChunkService chunkService = p_ctx.getDXRAMServiceAccessor().getService(ChunkService.class);
        ChunkLocalService chunkLocalService = p_ctx.getDXRAMServiceAccessor().getService(ChunkLocalService.class);

        Vertex vert = new Vertex(p_cid);
        chunkLocalService.getLocal().get(vert);

        BigDecimal b = new BigDecimal(vert.getPR1());
        //System.out.println(vert.get_name() + " * " + BigDecimal.valueOf(vert.getM_currPR()).toPlainString());
        System.out.println(vert.get_name() + " * " + b.toString());
    }

    @Override
    public void handleSignal(Signal p_signal) {

    }

    @Override
    public void exportObject(Exporter p_exporter) {
        p_exporter.writeString(m_outDir);
    }

    @Override
    public void importObject(Importer p_importer) {
        m_outDir = p_importer.readString(m_outDir);
    }

    @Override
    public int sizeofObject() {
        return ObjectSizeUtil.sizeofString(m_outDir);
    }
}
