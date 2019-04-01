package de.hhu.bsinfo.dxapp.jobs;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import de.hhu.bsinfo.dxram.job.AbstractJob;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

public class PrStatisticsJob extends AbstractJob {

    private String m_outDir;
    private int m_vertexCount;
    private long m_InputTime;
    private long m_ExecutionTime;
    private int m_NumRounds;
    private double[] m_PRerrs;

    public PrStatisticsJob() {
    }

    public PrStatisticsJob(String p_outDir,int p_vertexCount ,long p_InputTime, long p_ExecutionTime, int p_NumRounds,double[] p_PRerrs) {
        m_outDir = p_outDir;
        m_vertexCount = p_vertexCount;
        m_InputTime = p_InputTime;
        m_ExecutionTime = p_ExecutionTime;
        m_NumRounds = p_NumRounds;
        m_PRerrs = p_PRerrs;

    }

    @Override
    public void execute() {
        String filename = m_outDir + "/" + "statistics.out";
        File outFile = new File(filename);
        try {
            outFile.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
        Path p = Paths.get(filename);

        try (BufferedWriter writer = Files.newBufferedWriter(p))
        {
            writer.write("#Statistics for PageRank Run " + m_outDir + "\n\n");
            writer.write("NUM_VERTICES\t" + m_vertexCount + "\n");
            writer.write("NUM_ROUNDS\t" + m_NumRounds + "\n");
            String InputTime = String.format("%.4f",(double)m_InputTime/(double)1000000000);
            String ExecutionTime = String.format("%.4f",(double)m_ExecutionTime/(double)1000000000);
            writer.write("INPUT_TIME\t" + InputTime + "s" + "\n");
            writer.write("EXECUTION_TIME\t" + ExecutionTime + "s" + "\n");

            writer.write("--------ROUNDS--------\n");
            writer.write("Round\tError\n");
            for (int i = 0; i < m_PRerrs.length; i++) {
                //String PRsum = String.format("%.4f", m_PRsums[i]);
                String PRerr = String.format("%.8f", m_PRerrs[i]);
                //String voteRatio = String.format("%.2f", (double)m_votes[i]/(double)m_vertexCount);
                writer.write((i+1) + "\t" + PRerr + "\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void importObject(Importer p_importer) {
        super.importObject(p_importer);
        m_outDir = p_importer.readString(m_outDir);
        m_vertexCount = p_importer.readInt(m_vertexCount);
        m_InputTime = p_importer.readLong(m_InputTime);
        m_ExecutionTime = p_importer.readLong(m_ExecutionTime);
        m_NumRounds = p_importer.readInt(m_NumRounds);
        m_PRerrs = p_importer.readDoubleArray(m_PRerrs);
    }

    @Override
    public void exportObject(Exporter p_exporter) {
        super.exportObject(p_exporter);
        p_exporter.writeString(m_outDir);
        p_exporter.writeInt(m_vertexCount);
        p_exporter.writeLong(m_InputTime);
        p_exporter.writeLong(m_ExecutionTime);
        p_exporter.writeInt(m_NumRounds);
        p_exporter.writeDoubleArray(m_PRerrs);
    }

    @Override
    public int sizeofObject() {
        return super.sizeofObject() + ObjectSizeUtil.sizeofString(m_outDir) + Integer.BYTES * 2
                + Long.BYTES * 2 + ObjectSizeUtil.sizeofDoubleArray(m_PRerrs);
    }
}
