package de.hhu.bsinfo.dxapp.tasks;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import de.hhu.bsinfo.dxapp.chunk.Vertex;
import de.hhu.bsinfo.dxapp.chunk.MetaChunk;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.ms.Signal;
import de.hhu.bsinfo.dxram.ms.Task;
import de.hhu.bsinfo.dxram.ms.TaskContext;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

/**
 * Reads an edgelist file into Chunks
 */

public class ReadLumpInEdgeListTask implements Task {

    private String m_file;
    private int m_vertexCnt;

    public ReadLumpInEdgeListTask(){}

    /**
     * @param p_file File to Read
     * @param p_vertexCnt Total number of vertices in the file
     */

    public ReadLumpInEdgeListTask(String p_file, int p_vertexCnt){
        m_file = p_file;
        m_vertexCnt = p_vertexCnt;
    }

    @Override
    public int execute(TaskContext taskContext) {
        ChunkService chunkService = taskContext.getDXRAMServiceAccessor().getService(ChunkService.class);
        ChunkLocalService chunkLocalService = taskContext.getDXRAMServiceAccessor().getService(ChunkLocalService.class);

        short mySlaveID = taskContext.getCtxData().getSlaveId();
        short myNodeID = taskContext.getCtxData().getOwnNodeId();
        short[] slaveIDs = taskContext.getCtxData().getSlaveNodeIds();
        int[] outDegrees = new int[m_vertexCnt];
        Vertex[] localVertices = new Vertex[localVertexCnt(m_vertexCnt,mySlaveID,slaveIDs.length)];
        int vertexNum = 0;
        int localVertexCount = 0;
        int edges = 0;
        try(BufferedReader br = new BufferedReader(new FileReader(m_file))){
            String line;

            while ((line = br.readLine()) != null){
                String[] split = line.split(" ");

                if (vertexNum % slaveIDs.length == mySlaveID){
                    localVertices[localVertexCount] = new Vertex(vertexNum + 1, m_vertexCnt);
                    localVertexCount++;
                }

                if (Integer.parseInt(split[0]) == 0){
                    vertexNum++;
                    continue;
                }

                for (int i = 0; i < split.length; i++) {
                    outDegrees[Integer.parseInt(split[i]) - 1]++;
                }

                edges += split.length;
                vertexNum++;
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("OutDeg read!");

        localVertexCount = 0;
        vertexNum = 0;
        int inVertex;
        try(BufferedReader br = new BufferedReader(new FileReader(m_file))){
            String line;

            while ((line = br.readLine()) != null){
                if (vertexNum % slaveIDs.length == mySlaveID){
                    localVertices[localVertexCount].setOutDeg(outDegrees[vertexNum]);
                    localVertices[localVertexCount].invokeVertexPR(m_vertexCnt);
                    String[] split = line.split(" ");

                    if (outDegrees[vertexNum] != 0){
                        if (Integer.parseInt(split[0]) == 0){
                            vertexNum++;
                            localVertexCount++;
                            continue;
                        }

                        for (int i = 0; i < split.length; i++) {
                            inVertex = Integer.parseInt(split[i]);

                            if (outDegrees[inVertex - 1] != 0){
                                localVertices[localVertexCount].addInEdge(correspondingChunkID(inVertex,slaveIDs));
                            }
                        }
                    } else {
                        if (Integer.parseInt(split[0]) == 0){
                            localVertexCount++;
                            vertexNum++;
                            continue;
                        }

                        for (int i = 0; i < split.length; i++) {
                            inVertex = Integer.parseInt(split[i]);
                            localVertices[localVertexCount].addInEdge(correspondingChunkID(inVertex,slaveIDs));
                        }
                    }
                    localVertexCount++;
                }
                vertexNum++;
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("Vertices created!");
        chunkLocalService.createLocal().create(localVertices);
        chunkService.put().put(localVertices);
        System.out.println("Chunks created!");

        MetaChunk vc = new MetaChunk(m_vertexCnt,edges);
        chunkService.create().create(myNodeID,vc);
        chunkService.put().put(vc);

        return 0;
    }

    private long correspondingChunkID(int p_vertex, short[] slaveIDs){
        int slaveCnt = slaveIDs.length;
        short nid = slaveIDs[((short) ((p_vertex-1) % slaveCnt))];
        long lid = (long) (((p_vertex-1) / slaveCnt) + 1);
        return ChunkID.getChunkID(nid,lid);
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
        exporter.writeString(m_file);
        exporter.writeInt(m_vertexCnt);
    }

    @Override
    public void importObject(Importer importer) {
        m_file = importer.readString(m_file);
        m_vertexCnt = importer.readInt(m_vertexCnt);
    }

    @Override
    public int sizeofObject() {
        return ObjectSizeUtil.sizeofString(m_file) + Integer.BYTES;
    }
}