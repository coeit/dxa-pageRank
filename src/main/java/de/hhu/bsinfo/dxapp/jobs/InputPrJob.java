package de.hhu.bsinfo.dxapp.jobs;

import de.hhu.bsinfo.dxmem.data.ChunkByteArray;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxapp.chunk.*;
import de.hhu.bsinfo.dxram.job.AbstractJob;
//import de.hhu.bsinfo.dxram.job.Job;
import de.hhu.bsinfo.dxram.ms.ComputeRole;
import de.hhu.bsinfo.dxram.ms.MasterSlaveComputeService;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxutils.NodeID;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class InputPrJob extends AbstractJob {

    //public static final short MS_TYPE_ID = 3;

    private String m_filename;
    private int m_vertexCnt;

    public InputPrJob() {}

    public InputPrJob(String p_filename, int p_vertexCnt){
        m_filename = p_filename;
        m_vertexCnt = p_vertexCnt;
    }

    /*public InputJob(String p_filename){
        m_filename = p_filename;
    }*/


    @Override
    public void execute() {
        ChunkService chunkService = getService(ChunkService.class);
        MasterSlaveComputeService computeService = getService(MasterSlaveComputeService.class);

        ArrayList<Short> slaveIDs = computeService.getStatusMaster((short) 0).getConnectedSlaves();
        /**Vertex[][] vertices = new vertex[slaveIDs.size()][slaveIDvertexCnt]**/
        Vertex[] vertices = new Vertex[m_vertexCnt];

        try(BufferedReader br = new BufferedReader(new FileReader(m_filename))){
            String line;
            int v1,v2;
            while ((line = br.readLine()) != null){
                String[] split = line.split(" ");
                v1 = Integer.parseInt(split[0]) - 1;
                v2 = Integer.parseInt(split[1]) - 1; //ERROR CHECK

                if(vertices[v1] == null){
                    vertices[v1] = new Vertex(v1 + 1);
                    vertices[v1].invokeVertexPR(m_vertexCnt);
                }

                if(vertices[v2] == null){
                    vertices[v2] = new Vertex(v2 + 1);
                    vertices[v2].invokeVertexPR(m_vertexCnt);
                }

                vertices[v1].increment_outDeg(1,m_vertexCnt);
                vertices[v2].addInEdge(correspondingChunkID(v1 + 1, slaveIDs));
                //System.out.println(ChunkID.toHexString(correspondingChunkID(v1 + 1, slaveIDs)) + " " + ChunkID.toHexString(correspondingChunkID(v2 + 1, slaveIDs)));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        int slaveIndex = 0;

        for (int i = 0; i < vertices.length; i++) {
            if(vertices[i].getOutDeg() == m_vertexCnt){
                for (int j = 0; j < vertices.length; j++) {
                    if(j != i){
                        vertices[j].addInEdge(correspondingChunkID(i + 1,slaveIDs));
                    }
                }
            }
        }

        for (Vertex vertex : vertices){
            chunkService.create().create(slaveIDs.get(slaveIndex % slaveIDs.size()),vertex);
            System.out.println(vertex.get_name() + " :: " + ChunkID.toHexString(vertex.getID()) + " " + vertex.getOutDeg() + " PR1: " + vertex.getPR1() + " PR2: " + vertex.getPR2());
            for (int i = 0; i < vertex.getM_inEdges().length; i++) {
                System.out.print(ChunkID.toHexString(vertex.getM_inEdges()[i]) + " ");
            }
            System.out.println();
            //m_nameService.register(chunkMap.get(vertexPR), vertexPR.toString());
            chunkService.put().put(vertex);
            slaveIndex++;
        }
    }

    public long correspondingChunkID(int p_vertex, ArrayList<Short> slaveIDs){
        int slaveCnt = slaveIDs.size();
        short nid = slaveIDs.get((short) ((p_vertex-1) % slaveCnt));
        long lid = (long) (((p_vertex-1) / slaveCnt) + 1);
        return ChunkID.getChunkID(nid,lid);
    }

    /*public int get_vertexCount(){
        return m_vertexCount;
    }*/

    @Override
    public void importObject(final Importer p_importer) {
        super.importObject(p_importer);

        m_filename = p_importer.readString(m_filename);
        m_vertexCnt = p_importer.readInt(m_vertexCnt);
        //m_vertexCount = p_importer.readInt(m_vertexCount);
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        super.exportObject(p_exporter);

        p_exporter.writeString(m_filename);
        p_exporter.writeInt(m_vertexCnt);
        //p_exporter.writeInt(m_vertexCount);
    }

    @Override
    public int sizeofObject() {
        return super.sizeofObject() + ObjectSizeUtil.sizeofString(m_filename) + Integer.BYTES;
    }

}
