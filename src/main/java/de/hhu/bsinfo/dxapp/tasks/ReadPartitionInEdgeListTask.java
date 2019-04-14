package de.hhu.bsinfo.dxapp.tasks;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.TreeMap;

import de.hhu.bsinfo.dxapp.chunk.Vertex;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxmem.data.ChunkLockOperation;
import de.hhu.bsinfo.dxmem.operations.Read;
import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.ms.MasterSlaveComputeService;
import de.hhu.bsinfo.dxram.ms.Signal;
import de.hhu.bsinfo.dxram.ms.Task;
import de.hhu.bsinfo.dxram.ms.TaskContext;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

public class ReadPartitionInEdgeListTask implements Task {

    private String m_file;
    private int m_vertexCnt;

    public ReadPartitionInEdgeListTask(){}

    public ReadPartitionInEdgeListTask(String p_file, int p_vertexCnt){
        m_file = p_file;
        m_vertexCnt = p_vertexCnt;
    }

    @Override
    public int execute(TaskContext taskContext) {

        ChunkService chunkService = taskContext.getDXRAMServiceAccessor().getService(ChunkService.class);
        ChunkLocalService chunkLocalService = taskContext.getDXRAMServiceAccessor().getService(ChunkLocalService.class);
        MasterSlaveComputeService computeService = taskContext.getDXRAMServiceAccessor().getService(MasterSlaveComputeService.class);

        short mySlaveID = taskContext.getCtxData().getSlaveId();
        short[] slaveIDs = taskContext.getCtxData().getSlaveNodeIds();


        TreeMap<Integer,ReadVertex> vertexMap = new TreeMap<>();
        //ReadVertex[] vertexMap = new ReadVertex[0];
        //ArrayList<ReadVertex> localReadVertices = new ArrayList<>();
        int[] partitionIndexCounter = new int[slaveIDs.length];
        int[] partitionIndex = new int[m_vertexCnt];
        int[] outDegrees = new int[m_vertexCnt];
        int vertexNumber = 0;
        int partition;
        try(BufferedReader br = new BufferedReader(new FileReader(m_file))){
            String line;

            while ((line = br.readLine()) != null){
                String[] split = line.split(" ");


                partition = Integer.parseInt(split[0]);
                partitionIndex[vertexNumber] = partition + partitionIndexCounter[partition] * slaveIDs.length;
                partitionIndexCounter[partition]++;

                for (int i = 1; i < split.length; i++) {
                    outDegrees[Integer.parseInt(split[i]) - 1]++;
                }

                if (partition == mySlaveID) {
                    ReadVertex tmp = new ReadVertex();
                    for (int i = 1; i < split.length; i++) {
                        tmp.m_inEdges.add(Integer.parseInt(split[i]));
                    }
                    vertexMap.put(vertexNumber + 1,tmp);

                }

                if(vertexNumber % 100000 == 0){
                    System.out.println(vertexNumber);
                }

                vertexNumber++;
            }

        } catch (IOException e) {
           e.printStackTrace();
        }

        /*for (int i = 0; i < outDegrees.length; i++) {
            System.out.println(i + " : " + outDegrees[i]);
        }

        for (int i = 0; i < partitionIndex.length; i++) {
            System.out.println(i + " + " + partitionIndex[i]);
        }*/

        ArrayList<Integer> danglingNodes = new ArrayList<>();

        for (int i = 0; i < outDegrees.length; i++) {
            if (outDegrees[i] == 0){
                danglingNodes.add(i + 1);
            }
        }
        System.out.println("dangling Nodes arr done!");

        for (int localVertex : vertexMap.keySet()){
            vertexMap.get(localVertex).m_inEdges.addAll(danglingNodes);
            if(outDegrees[localVertex - 1] != 0){
                vertexMap.get(localVertex).m_outdeg = outDegrees[localVertex - 1];
            } else {
                vertexMap.get(localVertex).m_outdeg = m_vertexCnt - 1;
            }

        }
        System.out.println("outdegs + dangling in done!");


        /*for (int i = 0; i < outDegrees.length; i++) {
            if (outDegrees[i] == 0){
                for (int localVertex : vertexMap.keySet()){
                    if (i + 1 != localVertex){
                        vertexMap.get(localVertex).m_inEdges.add(i + 1);
                    }
                }
            }
            if (vertexMap.containsKey(i + 1)){
                if(outDegrees[i] == 0) {
                    vertexMap.get(i + 1).setOutdeg(m_vertexCnt - 1);
                } else {
                    vertexMap.get(i + 1).setOutdeg(outDegrees[i]);
                }

            }
            if(i % 10 == 0){
                System.out.print(".");
            }

        }*/

        System.out.println("\nOutdegrees added!");


        for (int vertexNum : vertexMap.keySet()){
            System.out.println(vertexNum + " " + vertexMap.get(vertexNum).m_outdeg + " :: " + vertexMap.get(vertexNum).m_inEdges.toString());
            ReadVertex readVertex = vertexMap.get(vertexNum);
            Vertex vertex = new Vertex(vertexNum);
            long[] tmpEdges = new long[readVertex.m_inEdges.size()];
            for (int i = 0; i < tmpEdges.length; i++) {
                if(readVertex.m_inEdges.get(i) != vertexNum){
                    long correspondingCid = correspondingChunkID(partitionIndex[readVertex.m_inEdges.get(i) - 1], slaveIDs);
                    tmpEdges[i] = correspondingCid;
                }
            }
            vertex.addInEdges(tmpEdges);
            vertex.setOutDeg(readVertex.m_outdeg);
            vertex.invokeVertexPR(m_vertexCnt);
            chunkLocalService.createLocal().create(vertex);
            chunkService.put().put(vertex);
            System.out.print(vertex.get_name() + " " + ChunkID.toHexString(vertex.getID()) + " " + vertex.getOutDeg() + " ++ ");
            for (int i = 0; i < vertex.getM_inEdges().length; i++) {
                System.out.print(ChunkID.toHexString(vertex.getM_inEdges()[i]) + " ");
            }
            System.out.println(vertex.getPageRank(0));
            //vertexMap.remove(vertexNum);
        }
        System.out.println("Chunks created!");






        return 0;
    }


    public long correspondingChunkID(int p_vertex, short[] slaveIDs){

        short nid = slaveIDs[p_vertex % slaveIDs.length];
        long lid = (long) p_vertex / slaveIDs.length + 1;
        return ChunkID.getChunkID(nid,lid);
        /*short[] arr = new short[slaveIDs.length];
        for (int i = 0; i < slaveIDs.length - 1; i++) {
            arr[i] = (short) (i + 1);
        }

        int slaveCnt = slaveIDs.length;
        short nid = slaveIDs[arr[(short) ((p_vertex-1) % slaveCnt)]];
        long lid = (long) (((p_vertex-1) / slaveCnt) + 1);
        return ChunkID.getChunkID(nid,lid);*/
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
