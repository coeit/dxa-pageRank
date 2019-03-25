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
        String myFile = null;
        for (String s : fileSplit){
            if(s.contains("split_" + (slaveIDs.length - 1)) && mySlaveID == 0){
                myFile = s;
                break;
            } else if(s.contains("split_" + (mySlaveID -1))){
                myFile = s;
            }
        }
        if(myFile == null){
            System.out.println("Split file " + mySlaveID + " not found!");
        }
        //String myFile = fileSplit[mySlaveID];
        System.out.println("MyFile: " + myFile + ", mySlaveID: " + mySlaveID);
        int[] outDegrees = new int[m_vertexCnt];
        System.out.println("local V cnt: " + localVertexCnt);
        try(BufferedReader br = new BufferedReader(new FileReader(myFile))){
            String line;
            int cnt = 0;
            int v1,v2,v2i,v2old;
            if(mySlaveID == 0){
                v2old = slaveIDs.length;
            } else {
                v2old = mySlaveID;
            }
            while ((line = br.readLine()) != null){
                String[] split = line.split(" ");
                v1 = Integer.parseInt(split[0]) -1;
                v2i = Integer.parseInt(split[1]) -1;
                v2 = (int) Math.ceil((Double.parseDouble(split[1]) - 2)/(double) slaveIDs.length); //ERROR CHECK
                if(localVertices[v2] == null){
                    cnt++;
                    localVertices[v2] = new Vertex(v2i + 1);
                    localVertices[v2].invokeVertexPR(m_vertexCnt);
                    if (v2old + slaveIDs.length != v2i && v2old != v2i){
                        int skipped = (v2i - v2old)/slaveIDs.length - 1;
                        for (int i = 1; i <= skipped; i++) {
                            localVertices[v2 - i] = new Vertex(v2i + 1 - i * slaveIDs.length);
                            localVertices[v2 - i].invokeVertexPR(m_vertexCnt);
                            cnt++;
                        }

                    }
                    v2old = v2i;
                }

                outDegrees[v1]++;
                localVertices[v2].addInEdge(correspondingChunkID(v1 + 1, slaveIDs));
                //System.out.println(ChunkID.toHexString(correspondingChunkID(v1 + 1, slaveIDs)) + " " + ChunkID.toHexString(correspondingChunkID(v2i + 1, slaveIDs)));
            }
            System.out.println("CNt:" + cnt);
            while(cnt < localVertexCnt){
                System.out.println(cnt);
                localVertices[cnt] = new Vertex(localVertices[cnt-1].get_name() + slaveIDs.length);
                cnt++;
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        for (int i = 0; i < localVertices.length; i++) {
            //chunkLocalService.getLocal().get(localVertices[i]);
            System.out.println(localVertices[i].get_name() + " :: " + ChunkID.toHexString(localVertices[i].getID()) + " " + localVertices[i].getOutDeg());
            /*for (int j = 0; j < localVertices[i].getM_inEdges().length; j++) {
                System.out.print(ChunkID.toHexString(localVertices[i].getM_inEdges()[j]) + " ");
            }
            System.out.println("\n" + localVertices[i].getPR1() + " " + localVertices[i].getPR2());*/
        }

        chunkLocalService.createLocal().create(localVertices);
        chunkService.put().put(localVertices);

        /*for (int i = 0; i < localVertices.length; i++) {
            System.out.println(localVertices[i].get_name() + " :: " + ChunkID.toHexString(localVertices[i].getID()));
        }*/

        for (int i = 0; i < outDegrees.length; i++) {
            if(outDegrees[i] == 0){
                continue;
            }
            Vertex vertex = new Vertex(correspondingChunkID(i+1, slaveIDs));
            chunkService.get().get(vertex, ChunkLockOperation.WRITE_LOCK_ACQ_PRE_OP);
            vertex.increment_outDeg(outDegrees[i],m_vertexCnt);
            chunkService.put().put(vertex,ChunkLockOperation.WRITE_LOCK_REL_POST_OP);
        }

        /*for (int i = 0; i < localVertices.length; i++) {
            chunkLocalService.getLocal().get(localVertices[i]);
            System.out.println(localVertices[i].get_name() + " :: " + ChunkID.toHexString(localVertices[i].getID()) + " " + localVertices[i].getOutDeg());
            for (int j = 0; j < localVertices[i].getM_inEdges().length; j++) {
                System.out.print(ChunkID.toHexString(localVertices[i].getM_inEdges()[j]) + " ");
            }
            System.out.println("\n" + localVertices[i].getPR1() + " " + localVertices[i].getPR2());
        }*/



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
