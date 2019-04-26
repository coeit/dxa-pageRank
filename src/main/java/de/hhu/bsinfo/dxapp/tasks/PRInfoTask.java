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
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import java.math.BigDecimal;

public class PRInfoTask implements Task {

    String m_outDir;
    int m_round;
    boolean m_synthetic;

    public PRInfoTask(){
    }

    public PRInfoTask(String p_outDir, int p_round, boolean p_synthetic){
        m_outDir = p_outDir;
        m_round = p_round;
        m_synthetic = p_synthetic;
    }

    @Override
    public int execute(TaskContext p_ctx) {
        ChunkService chunkService = p_ctx.getDXRAMServiceAccessor().getService(ChunkService.class);
        BootService bootService = p_ctx.getDXRAMServiceAccessor().getService(BootService.class);
        String outPath = m_outDir + "/" + NodeID.toHexStringShort(bootService.getNodeID()) + ".pageRank";
        System.out.println(outPath);
        File outFile = new File(outPath);
        try {
            outFile.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
        Path p = Paths.get(outPath);

        Iterator<Long> localchunks = chunkService.cidStatus().getAllLocalChunkIDRanges(bootService.getNodeID()).iterator();
        //Spliterator<Long> localchunks = chunkService.cidStatus().getAllLocalChunkIDRanges(bootService.getNodeID()).spliterator();
        localchunks.next();

        Vertex[] localVertices = new Vertex[(int)chunkService.status().getStatus(bootService.getNodeID()).getLIDStoreStatus().getCurrentLIDCounter()];

        for (int i = 0; i < localVertices.length; i++) {
            localVertices[i] = new Vertex(localchunks.next());
        }

        chunkService.get().get(localVertices);

        try (BufferedWriter writer = Files.newBufferedWriter(p))
        {
            Stream.of(localVertices).forEach(localVertex -> {
                try {
                    //printInfo(localVertex, writer);
                    if (!m_synthetic){
                        writer.write(localVertex.get_name() + " " + BigDecimal.valueOf(localVertex.getPageRank(m_round)).toPlainString() + "\n");

                    } else {
                        writer.write(ChunkID.toHexString(localVertex.getID()) + " " + BigDecimal.valueOf(localVertex.getPageRank(m_round)).toPlainString() + "\n");

                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }

        return 0;
    }

    public void printInfo(Vertex vertex, Writer writer) throws IOException {
        writer.write(vertex.get_name() + " - " + ChunkID.toHexString(vertex.getID()) + " - " + vertex.getOutDeg() + " : ");
        for (int i = 0; i < vertex.getM_inEdges().length; i++) {
            writer.write(ChunkID.toHexString(vertex.getM_inEdges()[i]) + " ");
        }
        writer.write("\n");
    }

    @Override
    public void handleSignal(Signal p_signal) {

    }

    @Override
    public void exportObject(Exporter p_exporter) {
        p_exporter.writeString(m_outDir);
        p_exporter.writeInt(m_round);
    }

    @Override
    public void importObject(Importer p_importer) {
        m_outDir = p_importer.readString(m_outDir);
        m_round = p_importer.readInt(m_round);
    }

    @Override
    public int sizeofObject() {
        return ObjectSizeUtil.sizeofString(m_outDir) + Integer.BYTES;
    }
}
