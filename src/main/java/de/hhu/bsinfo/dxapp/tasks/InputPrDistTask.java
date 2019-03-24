package de.hhu.bsinfo.dxapp.tasks;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import de.hhu.bsinfo.dxapp.chunk.Vertex;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxmem.data.ChunkLockOperation;
import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.ms.Signal;
import de.hhu.bsinfo.dxram.ms.Task;
import de.hhu.bsinfo.dxram.ms.TaskContext;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

public class InputPrDistTask implements Task {

    private String m_files;
    private int m_vertexCnt;

    public InputPrDistTask(){}

    public InputPrDistTask(String p_files, int p_vertexCnt){
        m_files = p_files;
        m_vertexCnt = p_vertexCnt;
    }

    @Override
    public int execute(TaskContext taskContext) {

        ChunkService chunkService = taskContext.getDXRAMServiceAccessor().getService(ChunkService.class);
        ChunkLocalService chunkLocalService = taskContext.getDXRAMServiceAccessor().getService(ChunkLocalService.class);

        short mySlaveID = taskContext.getCtxData().getSlaveId();
        short[] slaveIDs = taskContext.getCtxData().getSlaveNodeIds();

        int localVertexCnt = localVertexCnt(m_vertexCnt, mySlaveID, taskContext.getCtxData().getSlaveNodeIds().length);
        Vertex[] localVertices = new Vertex[localVertexCnt];

        String[] fileSplit = m_files.split("@");
        String myFile = fileSplit[mySlaveID];

        int[] outDegrees = new int[m_vertexCnt];

        try(BufferedReader br = new BufferedReader(new FileReader(myFile))){
            String line;
            int v1,v2;
            while ((line = br.readLine()) != null){
                String[] split = line.split(" ");
                v1 = Integer.parseInt(split[0]);
                v2 = (int) Math.ceil(Double.parseDouble(split[1]) /(double) taskContext.getCtxData().getSlaveNodeIds().length); //ERROR CHECK

                if(localVertices[v2] == null){
                    localVertices[v2] = new Vertex(v2);
                    localVertices[v2].invokeVertexPR(m_vertexCnt);
                }

                outDegrees[v1]++;
                localVertices[v2].addInEdge(correspondingChunkID(v1, slaveIDs));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        chunkLocalService.createLocal().create(localVertices);
        chunkService.put().put(localVertices);

        for (int i = 0; i < outDegrees.length; i++) {
            if(outDegrees[i] == 0){
                continue;
            }
            Vertex vertex = new Vertex(correspondingChunkID(i+1, slaveIDs));
            chunkService.get().get(vertex, ChunkLockOperation.WRITE_LOCK_ACQ_PRE_OP);
            vertex.increment_outDeg(outDegrees[i],m_vertexCnt);
            chunkService.put().put(vertex,ChunkLockOperation.WRITE_LOCK_REL_POST_OP);
        }



        return 0;
    }

    public int localVertexCnt(int p_totalVertexCnt, int p_slaveID, int p_numSlaves){
        int mod = p_totalVertexCnt % p_numSlaves;
        double div = (double)p_totalVertexCnt/(double)p_numSlaves;
        if(p_slaveID <= mod && p_slaveID != 0){
            return (int) Math.ceil(div);
        }
        return (int) Math.floor(div);
    }

    public long correspondingChunkID(int p_vertex, short[] slaveIDs){
        int slaveCnt = slaveIDs.length;
        short nid = slaveIDs[(short) ((p_vertex-1) % slaveCnt)];
        long lid = (long) (((p_vertex-1) / slaveCnt) + 1);
        return ChunkID.getChunkID(nid,lid);
    }

    @Override
    public void handleSignal(Signal signal) {

    }

    @Override
    public void exportObject(Exporter p_exporter) {
        p_exporter.writeString(m_files);
        p_exporter.writeInt(m_vertexCnt);
    }

    @Override
    public void importObject(Importer p_importer) {
        m_files = p_importer.readString(m_files);
        m_vertexCnt = p_importer.readInt(m_vertexCnt);
    }

    @Override
    public int sizeofObject() {
        return ObjectSizeUtil.sizeofString(m_files) + Integer.BYTES;
    }
}