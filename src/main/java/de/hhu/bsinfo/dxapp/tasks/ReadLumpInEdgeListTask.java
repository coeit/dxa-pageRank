package de.hhu.bsinfo.dxapp.tasks;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import de.hhu.bsinfo.dxapp.chunk.LocalDanglingChunks;
import de.hhu.bsinfo.dxapp.chunk.LocalNonDanglingChunks;
import de.hhu.bsinfo.dxapp.chunk.Vertex;
import de.hhu.bsinfo.dxmem.data.ChunkID;
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

public class ReadLumpInEdgeListTask implements Task {

    private String m_file;
    private int m_vertexCnt;

    public ReadLumpInEdgeListTask(){}

    public ReadLumpInEdgeListTask(String p_file, int p_vertexCnt){
        m_file = p_file;
        m_vertexCnt = p_vertexCnt;
    }

    @Override
    public int execute(TaskContext taskContext) {
        ChunkService chunkService = taskContext.getDXRAMServiceAccessor().getService(ChunkService.class);
        ChunkLocalService chunkLocalService = taskContext.getDXRAMServiceAccessor().getService(ChunkLocalService.class);
        MasterSlaveComputeService computeService = taskContext.getDXRAMServiceAccessor().getService(MasterSlaveComputeService.class);
        NameserviceService nameService = taskContext.getDXRAMServiceAccessor().getService(NameserviceService.class);

        short mySlaveID = taskContext.getCtxData().getSlaveId();
        System.out.println("myID:" + mySlaveID);
        short[] slaveIDs = taskContext.getCtxData().getSlaveNodeIds();

        int[] outDegrees = new int[m_vertexCnt];
        Vertex[] localVertices = new Vertex[localVertexCnt(m_vertexCnt,mySlaveID,slaveIDs.length)];
        System.out.println("LocalVertices: " + localVertices.length);
        int vertexNum = 0;
        int localVertexCount = 0;
        try(BufferedReader br = new BufferedReader(new FileReader(m_file))){
            String line;

            while ((line = br.readLine()) != null){
                String[] split = line.split(" ");
                for (int i = 0; i < split.length; i++) {
                    outDegrees[Integer.parseInt(split[i]) - 1]++;
                }
                if (vertexNum % slaveIDs.length == mySlaveID){
                    localVertices[localVertexCount] = new Vertex(vertexNum + 1);
                    localVertexCount++;
                }
                vertexNum++;
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("OutDeg read!");

        localVertexCount = 0;
        vertexNum = 0;
        int inVertex;
        ArrayList<Long> localDanglingChunks = new ArrayList<>();
        ArrayList<Long> localNonDanglingChunks = new ArrayList<>();
        try(BufferedReader br = new BufferedReader(new FileReader(m_file))){
            String line;

            while ((line = br.readLine()) != null){
                if (vertexNum % slaveIDs.length == mySlaveID){
                    String[] split = line.split(" ");
                    if (outDegrees[vertexNum] != 0){
                        localNonDanglingChunks.add(correspondingChunkID(vertexNum + 1, slaveIDs));
                        for (int i = 0; i < split.length; i++) {
                            inVertex = Integer.parseInt(split[i]);
                            if (outDegrees[inVertex - 1] != 0){
                                localVertices[localVertexCount].addInEdge(correspondingChunkID(inVertex,slaveIDs));
                            }
                        }
                    } else {
                        localDanglingChunks.add(correspondingChunkID(vertexNum + 1, slaveIDs));
                        for (int i = 0; i < split.length; i++) {
                            inVertex = Integer.parseInt(split[i]);
                            localVertices[localVertexCount].addInEdge(correspondingChunkID(inVertex,slaveIDs));
                        }
                    }
                    localVertices[localVertexCount].setOutDeg(outDegrees[vertexNum]);
                    localVertices[localVertexCount].invokeVertexPR(m_vertexCnt);
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

        for (int i = 0; i < localVertices.length; i++) {
            System.out.print(localVertices[i].get_name() + " " + ChunkID.toHexString(localVertices[i].getID()) + " " + localVertices[i].getOutDeg() + " ++ ");

            for (int j = 0; j < localVertices[i].getM_inEdges().length; j++) {
                System.out.print(ChunkID.toHexString(localVertices[i].getM_inEdges()[j]) + " ");
            }
            System.out.println();
        }

        LocalNonDanglingChunks ndChunks = new LocalNonDanglingChunks(localNonDanglingChunks.stream().mapToLong(i -> i).toArray());
        LocalDanglingChunks dChunks = new LocalDanglingChunks(localDanglingChunks.stream().mapToLong(i -> i).toArray());

        chunkLocalService.createLocal().create(ndChunks);
        nameService.register(ndChunks,mySlaveID + "nd");
        chunkService.put().put(ndChunks);
        System.out.println(ndChunks.getID());

        chunkLocalService.createLocal().create(dChunks);
        nameService.register(dChunks,mySlaveID + "d");
        chunkService.put().put(dChunks);


        System.out.println("ChunkLists created!");

        System.out.println("NonDangling:");
        for (int i = 0; i < ndChunks.getLocalNonDanglingChunks().length; i++) {
            System.out.print(ChunkID.toHexString(ndChunks.getLocalNonDanglingChunks()[i]) + " ");
        }
        System.out.println();

        System.out.println("Dangling:");
        for (int i = 0; i < dChunks.getLocalDanglingChunks().length; i++) {
            System.out.print(ChunkID.toHexString(dChunks.getLocalDanglingChunks()[i]) + " ");
        }
        System.out.println();

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
    public void exportObject(Exporter p_exporter) {
        p_exporter.writeString(m_file);
        p_exporter.writeInt(m_vertexCnt);
    }

    @Override
    public void importObject(Importer p_importer) {
        m_file = p_importer.readString(m_file);
        m_vertexCnt = p_importer.readInt(m_vertexCnt);
    }

    @Override
    public int sizeofObject() {
        return ObjectSizeUtil.sizeofString(m_file) + Integer.BYTES;
    }
}
