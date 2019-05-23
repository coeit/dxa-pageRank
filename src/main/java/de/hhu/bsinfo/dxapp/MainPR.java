package de.hhu.bsinfo.dxapp;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import de.hhu.bsinfo.dxapp.chunk.MetaChunk;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxram.app.AbstractApplication;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.engine.DXRAMVersion;
import de.hhu.bsinfo.dxram.generated.BuildConfig;
import de.hhu.bsinfo.dxapp.jobs.*;
import de.hhu.bsinfo.dxram.job.*;
import de.hhu.bsinfo.dxram.ms.*;
import de.hhu.bsinfo.dxram.ms.script.TaskScript;
import de.hhu.bsinfo.dxapp.tasks.*;
import de.hhu.bsinfo.dxutils.Stopwatch;

/**
 * PageRank implemetation for DXRAM
 *
 * @author Constantin Eiteneuer, constantin.eiteneuer@hhu.de
 */
public class MainPR extends AbstractApplication {
    @Override
    public DXRAMVersion getBuiltAgainstVersion() {
        return BuildConfig.DXRAM_VERSION;
    }

    @Override
    public String getApplicationName() {
        return "dxa-PageRank";
    }

    @Override
    public void main(final String[] p_args) {

        if (p_args.length < 6){
            System.out.println("Not enough Arguments ... shutting down");
            System.out.println("Arguments: int vertexcnt double dampingfactor double errorthreshold int maxrounds boolean printPageRanks (String graphfile) / (double locality int MeanIndegree int randomSeed)");
        }

        int N = Integer.parseInt(p_args[0]);
        double DAMPING_FACTOR = Double.parseDouble(p_args[1]);
        double THRESHOLD = Double.parseDouble(p_args[2]);
        int MAX_ROUNDS = Integer.parseInt(p_args[3]);
        boolean printPR = Boolean.parseBoolean(p_args[4]);
        boolean isSynthetic = false;

        ChunkService chunkService = getService(ChunkService.class);
        MasterSlaveComputeService computeService = getService(MasterSlaveComputeService.class);
        JobService jobService = getService(JobService.class);

        ArrayList<Short> connectedSlaves = computeService.getStatusMaster((short) 0).getConnectedSlaves();

        Stopwatch stopwatch = new Stopwatch();

        String filename = "SYNTHETIC";
        double locality = 0.0;
        int meanInDeg = 0;

        /**Graph Input (Either File or Synthetic)**/

        if(p_args.length == 6) {
            filename = p_args[5];
            ReadLumpInEdgeListTask readLumpInEdgeListTask = new ReadLumpInEdgeListTask(filename, N);
            TaskScript inputTaskScript = new TaskScript(readLumpInEdgeListTask);
            TaskScriptState inputState = computeService.submitTaskScript(inputTaskScript, (short) 0);
            stopwatch.start();

            while (!inputState.hasTaskCompleted()) {
                try {
                    Thread.sleep(100);
                } catch (final InterruptedException ignore) {

                }
            }
            stopwatch.stop();
        } else {
            isSynthetic = true;
            CreateSyntheticGraphSeed createSyntheticGraph;
            locality = Double.parseDouble(p_args[5]);
            meanInDeg = Integer.parseInt(p_args[6]);
            int randomSeed = Integer.parseInt(p_args[7]);
            createSyntheticGraph = new CreateSyntheticGraphSeed(N, locality, meanInDeg, randomSeed);
            TaskScript inputTaskScript = new TaskScript(createSyntheticGraph);
            stopwatch.start();
            TaskScriptState inputState = computeService.submitTaskScript(inputTaskScript, (short) 0);
            stopwatch.start();

            while (!inputState.hasTaskCompleted()) {
                try {
                    Thread.sleep(100);
                } catch (final InterruptedException ignore) {

                }
            }

            stopwatch.stop();
        }

        long inputTime = stopwatch.getTime();
        long memUsage = 0;
        long edgeCnt = 0;
        System.out.println("GRAPH INPUT DONE...");
        MetaChunk[] metaChunks = new MetaChunk[connectedSlaves.size()];

        for (int i = 0; i < connectedSlaves.size(); i++) {
            metaChunks[i] = new MetaChunk(ChunkID.getChunkID(connectedSlaves.get(i),localVertexCnt(N,i,connectedSlaves.size()) + 1));
            chunkService.get().get(metaChunks[i]);
            memUsage += chunkService.status().getStatus(connectedSlaves.get(i)).getHeapStatus().getUsedSize().getBytes();
            edgeCnt += metaChunks[i].getEdgeCnt();
        }

        System.out.println("VERTICES: " + N);
        System.out.println("EDGES: " + edgeCnt);
        System.out.println("Memory: " + memUsage + "B");

        RunLumpPrRoundTask Run1 = new RunLumpPrRoundTask(N,DAMPING_FACTOR,0,false);
        RunLumpPrRoundTask Run2 = new RunLumpPrRoundTask(N,DAMPING_FACTOR,1,false);

        TaskScript taskScriptRun1 = new TaskScript(Run1);
        TaskScript taskScriptRun2 = new TaskScript(Run2);

        ArrayList<Double> roundPRerr = new ArrayList<>();

        int NumRounds = 0;
        double danglingPR;
        double PRerr;
        ArrayList<Long> iterationTimes = new ArrayList<>();
        TaskScriptState state;

        /**PageRank Iterations**/

        for (int i = 0; i < MAX_ROUNDS; i++) {
            danglingPR = 1;
            PRerr = 0;
            stopwatch.start();

            if(i % 2 == 0){
                state = computeService.submitTaskScript(taskScriptRun1, (short) 0);
            } else {
                state = computeService.submitTaskScript(taskScriptRun2, (short) 0);
            }

            while (!state.hasTaskCompleted()) {
                try {
                    Thread.sleep(100);
                } catch (final InterruptedException ignore) {

                }
            }

            chunkService.get().get(metaChunks);

            for (MetaChunk metaChunk : metaChunks) {
                PRerr += metaChunk.getPRerr();
                danglingPR = danglingPR - metaChunk.getPRsum();
            }

            for (MetaChunk metaChunk : metaChunks) {
                metaChunk.setPRsum(danglingPR);
            }

            chunkService.put().put(metaChunks);
            stopwatch.stop();

            roundPRerr.add(PRerr);
            iterationTimes.add(stopwatch.getTime());

            NumRounds++;

            System.out.println("ROUND\t" + NumRounds);
            System.out.println("TIME\t" + stopwatch.getTime());
            System.out.println("ERROR\t" + PRerr);

            if (PRerr <= THRESHOLD) {
                break;
            }

        }

        /**Restore dangling PageRanks**/

        RunLumpPrRoundTask calcDanglingPR = new RunLumpPrRoundTask(N,DAMPING_FACTOR,NumRounds % 2,true);
        TaskScript taskScriptCalcDanglingPR = new TaskScript(calcDanglingPR);
        state = computeService.submitTaskScript(taskScriptCalcDanglingPR,(short) 0);

        while (!state.hasTaskCompleted()) {
            try {
                Thread.sleep(100);
            } catch (final InterruptedException ignore) {

            }
        }

        /**print OutputFiles**/

        String outDir = createOutputDirs();

        if(printPR){
            PRInfoTask PRInfo = new PRInfoTask(outDir,NumRounds % 2, isSynthetic);
            TaskScript PRInfoTaskScript = new TaskScript(PRInfo);
            TaskScriptState PRInfoTaskScriptState = computeService.submitTaskScript(PRInfoTaskScript, (short) 0);
            while (!PRInfoTaskScriptState.hasTaskCompleted() && computeService.getStatusMaster((short) 0).getNumTasksQueued() != 0) {
                try {
                    Thread.sleep(100);
                } catch (final InterruptedException ignore) {

                }
            }
        }

        double[] roundPRerrArr = roundPRerr.stream().mapToDouble(i -> i).toArray();
        long[] iterationTimesArr = iterationTimes.stream().mapToLong(i -> i).toArray();

        double memUseMB = (double) memUsage / Math.pow(1024,2);
        PrStatisticsJob prStatisticsJob = new PrStatisticsJob(outDir,filename,N,edgeCnt,DAMPING_FACTOR,THRESHOLD,inputTime,iterationTimesArr,memUseMB,roundPRerrArr,locality,meanInDeg);
        jobService.pushJobRemote(prStatisticsJob, computeService.getStatusMaster((short) 0).getConnectedSlaves().get(0));
        jobService.waitForAllJobsToFinish();

    }

    public String createOutputDirs(){
        String HOME = System.getProperty("user.home");
        File PrOutDir = new File(HOME + "/" + "dxa-pageRank_out");

        if (!PrOutDir.exists()){
            PrOutDir.mkdir();
        }
        String out = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss").format(new Date());
        out = "pageRank_" + out;
        File outDir = new File(PrOutDir + "/" + out);
        outDir.mkdir();
        String ret = new String(PrOutDir + "/" + out);
        return ret;
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
    public void signalShutdown() {
        // Interrupt any flow of your application and make sure it shuts down.
        // Do not block here or wait for something to shut down. Shutting down of your application
        // must be execute asynchronously
    }
}
