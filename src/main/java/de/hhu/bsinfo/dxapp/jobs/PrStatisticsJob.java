package de.hhu.bsinfo.dxapp.jobs;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

import de.hhu.bsinfo.dxram.job.AbstractJob;
import de.hhu.bsinfo.dxram.ms.MasterSlaveComputeService;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

public class PrStatisticsJob extends AbstractJob {

    private String m_outDir;
    private String m_graphInput;
    private int m_vertexCount;
    private int m_edgeCount;
    private double m_damp;
    private double m_thr;
    private long m_InputTime;
    private long[] m_ExecutionTimes;
    private double m_memUsage;
    private double[] m_PRerrs;
    private double m_locality;
    private int m_meanIndeg;

    public PrStatisticsJob() {
    }

    public PrStatisticsJob(String p_outDir, String p_graphInput, int p_vertexCount, int p_edgeCount ,double p_damp, double p_thr ,long p_InputTime,
            long[] p_ExecutionTimes, double p_memUsage, double[] p_PRerrs, double p_locality, int p_meanIndeg) {
        m_outDir = p_outDir;
        m_graphInput = p_graphInput;
        m_vertexCount = p_vertexCount;
        m_edgeCount = p_edgeCount;
        m_damp = p_damp;
        m_thr = p_thr;
        m_InputTime = p_InputTime;
        m_ExecutionTimes = p_ExecutionTimes;
        m_memUsage = p_memUsage;
        m_PRerrs = p_PRerrs;
        m_locality = p_locality;
        m_meanIndeg = p_meanIndeg;
    }

    @Override
    public void execute() {
        MasterSlaveComputeService computeService = getService(MasterSlaveComputeService.class);
        int num_slaves = computeService.getStatusMaster((short) 0).getConnectedSlaves().size();
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
            writer.write("#Statistics for PageRank Run " + m_outDir + " | " + m_graphInput + "\n\n");
            writer.write("NUM_SLAVES\t" + num_slaves + "\n");
            writer.write("NUM_VERTICES\t" + m_vertexCount + "\n");
            writer.write("NUM_EDGES\t" + m_edgeCount + "\n");
            writer.write("DAMPING_VAL\t" + m_damp + "\n");
            writer.write("THRESHOLD\t" + m_thr + "\n");
            if(m_graphInput.equals("SYNTHETIC")){
                writer.write("LOCALITY\t" + m_locality + "\n");
                writer.write("MEAN_INDEG\t" + m_meanIndeg + "\n");
            }
            writer.write("NUM_ROUNDS\t" + m_ExecutionTimes.length + "\n");
            long timeSum = timeSum(m_ExecutionTimes);
            String InputTime = String.format("%.4f",(double)m_InputTime/(double)1000000000);
            String ExecutionTime = String.format("%.4f",(double)timeSum/(double)1000000000);
            writer.write("INPUT_TIME\t" + InputTime + "s" + "\n");
            writer.write("EXECUTION_TIME\t" + ExecutionTime + "s" + "\n");
            String memUse = String.format("%.4f",m_memUsage);

            writer.write("MEM_USAGE\t" + memUse + "MB" + "\n");
            writer.write("--------ROUNDS--------\n");
            writer.write("Round\tError\tTime\n");
            for (int i = 0; i < m_PRerrs.length; i++) {
                //String PRsum = String.format("%.4f", m_PRsums[i]);
                String PRerr = String.format("%.12f", m_PRerrs[i]);
                String time = String.format("%.4f",(double)m_ExecutionTimes[i]/(double)1000000000);
                //String voteRatio = String.format("%.2f", (double)m_votes[i]/(double)m_vertexCount);
                writer.write((i+1) + "\t" + PRerr + "\t" + time + "s\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private long timeSum (long[] p_times){
        long ret = 0;
        for (long time : p_times){
            ret += time;
        }
        return ret;
    }

    @Override
    public void importObject(Importer p_importer) {
        super.importObject(p_importer);
        m_outDir = p_importer.readString(m_outDir);
        m_graphInput = p_importer.readString(m_graphInput);
        m_vertexCount = p_importer.readInt(m_vertexCount);
        m_edgeCount = p_importer.readInt(m_edgeCount);
        m_damp = p_importer.readDouble(m_damp);
        m_thr = p_importer.readDouble(m_thr);
        m_InputTime = p_importer.readLong(m_InputTime);
        m_ExecutionTimes = p_importer.readLongArray(m_ExecutionTimes);
        m_memUsage = p_importer.readDouble(m_memUsage);
        m_PRerrs = p_importer.readDoubleArray(m_PRerrs);
        m_locality = p_importer.readDouble(m_locality);
        m_meanIndeg = p_importer.readInt(m_meanIndeg);
    }

    @Override
    public void exportObject(Exporter p_exporter) {
        super.exportObject(p_exporter);
        p_exporter.writeString(m_outDir);
        p_exporter.writeString(m_graphInput);
        p_exporter.writeInt(m_vertexCount);
        p_exporter.writeInt(m_edgeCount);
        p_exporter.writeDouble(m_damp);
        p_exporter.writeDouble(m_thr);
        p_exporter.writeLong(m_InputTime);
        p_exporter.writeLongArray(m_ExecutionTimes);
        p_exporter.writeDouble(m_memUsage);
        p_exporter.writeDoubleArray(m_PRerrs);
        p_exporter.writeDouble(m_locality);
        p_exporter.writeInt(m_meanIndeg);
    }

    @Override
    public int sizeofObject() {
        return super.sizeofObject() + ObjectSizeUtil.sizeofString(m_outDir) + ObjectSizeUtil.sizeofString(m_graphInput) + Integer.BYTES * 3
                + Long.BYTES + ObjectSizeUtil.sizeofLongArray(m_ExecutionTimes) + Double.BYTES * 4 + ObjectSizeUtil.sizeofDoubleArray(m_PRerrs);
    }
}
