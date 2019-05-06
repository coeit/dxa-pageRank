package de.hhu.bsinfo.dxapp.tasks;

import de.hhu.bsinfo.dxapp.chunk.Vertex;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.ms.Signal;
import de.hhu.bsinfo.dxram.ms.Task;
import de.hhu.bsinfo.dxram.ms.TaskContext;
import de.hhu.bsinfo.dxutils.NodeID;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.stream.Stream;

import java.math.BigDecimal;

/**
 * Task to write the final PageRank values to File
 */

public class PRInfoTask implements Task {

    private String m_outDir;
    private int m_round;
    private boolean m_synthetic;

    public PRInfoTask(){
    }

    /**
     * @param p_outDir Directory to write the files
     * @param p_round which PageRank Variable to read
     * @param p_synthetic true if synthetic Graph created with CreatSyntheticGraphSeed Task
     */

    public PRInfoTask(String p_outDir, int p_round, boolean p_synthetic){
        m_outDir = p_outDir;
        m_round = p_round;
        m_synthetic = p_synthetic;
    }

    @Override
    public int execute(TaskContext taskContext) {
        ChunkService chunkService = taskContext.getDXRAMServiceAccessor().getService(ChunkService.class);
        BootService bootService = taskContext.getDXRAMServiceAccessor().getService(BootService.class);

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
        localchunks.next();
        Vertex[] localVertices = new Vertex[(int)chunkService.status().getStatus(bootService.getNodeID()).getLIDStoreStatus().getCurrentLIDCounter() - 1];

        for (int i = 0; i < localVertices.length; i++) {
            localVertices[i] = new Vertex(localchunks.next());
        }

        chunkService.get().get(localVertices);

        try (BufferedWriter writer = Files.newBufferedWriter(p))
        {
            Stream.of(localVertices).forEach(localVertex -> {
                try {
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

    @Override
    public void handleSignal(Signal p_signal) {

    }

    @Override
    public void exportObject(Exporter p_exporter) {
        p_exporter.writeString(m_outDir);
        p_exporter.writeInt(m_round);
        p_exporter.writeBoolean(m_synthetic);
    }

    @Override
    public void importObject(Importer p_importer) {
        m_outDir = p_importer.readString(m_outDir);
        m_round = p_importer.readInt(m_round);
        m_synthetic = p_importer.readBoolean(m_synthetic);
    }

    @Override
    public int sizeofObject() {
        return ObjectSizeUtil.sizeofString(m_outDir) + Integer.BYTES + ObjectSizeUtil.sizeofBoolean();
    }
}
