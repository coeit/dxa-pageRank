package de.hhu.bsinfo.dxapp;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import de.hhu.bsinfo.dxapp.chunk.VoteChunk;
import de.hhu.bsinfo.dxmem.data.ChunkLockOperation;
import de.hhu.bsinfo.dxram.app.AbstractApplication;
//import de.hhu.bsinfo.dxram.app.Application;
import de.hhu.bsinfo.dxram.app.ApplicationCallbackHandler;
import de.hhu.bsinfo.dxram.app.ApplicationService;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxapp.chunk.IntegerChunk;
import de.hhu.bsinfo.dxram.engine.DXRAMVersion;
//import de.hhu.bsinfo.dxram.function.PRInputFunction;
import de.hhu.bsinfo.dxram.generated.BuildConfig;
import de.hhu.bsinfo.dxapp.jobs.*;
import de.hhu.bsinfo.dxram.job.*;
import de.hhu.bsinfo.dxram.ms.*;
import de.hhu.bsinfo.dxram.ms.script.TaskScript;
import de.hhu.bsinfo.dxapp.tasks.*;
import de.hhu.bsinfo.dxutils.Stopwatch;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;

/**
 * "Hello world" example DXRAM application
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 17.05.17
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
            System.out.println("Arguments: int vertexcnt double dampingfactor double errorthreshold int maxrounds boolean printPageRanks (String graphfile) / (double locality int MeanIndegree (int randomSeed))");

            signalShutdown();
        }

        int N = Integer.parseInt(p_args[0]);
        double DAMPING_FACTOR = Double.parseDouble(p_args[1]);
        double THRESHOLD = Double.parseDouble(p_args[2]);
        int MAX_ROUNDS = Integer.parseInt(p_args[3]);
        boolean printPR = Boolean.parseBoolean(p_args[4]);
        boolean isSynthetic = false;


        BootService bootService = getService(BootService.class);
        ChunkService chunkService = getService(ChunkService.class);
        NameserviceService nameService = getService(NameserviceService.class);
        MasterSlaveComputeService computeService = getService(MasterSlaveComputeService.class);
        JobService jobService = getService(JobService.class);



        IntegerChunk rdyCnt = new IntegerChunk();
        chunkService.create().create(bootService.getNodeID(),rdyCnt);
        chunkService.put().put(rdyCnt);

        IntegerChunk edgeCnt = new IntegerChunk();
        chunkService.create().create(bootService.getNodeID(),edgeCnt);
        chunkService.put().put(edgeCnt);

        Stopwatch stopwatch = new Stopwatch();
        System.out.println("len: "  + p_args.length);

        String filename = "SYNTHETIC";
        double locality = 0.0;
        int meanInDeg = 0;
        if(p_args.length == 6) {
            filename = p_args[5];
            ReadLumpInEdgeListTask readLumpInEdgeListTask = new ReadLumpInEdgeListTask(filename, N, edgeCnt.getID());
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
            CreateSyntheticGraph createSyntheticGraph;
            locality = Double.parseDouble(p_args[5]);
            meanInDeg = Integer.parseInt(p_args[6]);
            if(p_args.length == 8){
                createSyntheticGraph = new CreateSyntheticGraph(N, locality, meanInDeg, rdyCnt.getID(), edgeCnt.getID(), Integer.parseInt(p_args[7]));
            } else {
                createSyntheticGraph = new CreateSyntheticGraph(N,locality, meanInDeg, rdyCnt.getID(), edgeCnt.getID(), 0);
            }

            TaskScript inputTaskScript = new TaskScript(createSyntheticGraph);
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
        /*VoteChunk voteChunk = new VoteChunk(N);
        chunkService.create().create(bootService.getNodeID(),voteChunk);
        chunkService.put().put(voteChunk);*/
        VoteChunk[] voteChunks = new VoteChunk[computeService.getStatusMaster((short) 0).getConnectedSlaves().size()];
        //long[] voteChunkIDs = new long[computeService.getStatusMaster((short) 0).getConnectedSlaves().size()];
        int k = 0;
        for (short nodeID : computeService.getStatusMaster((short) 0).getConnectedSlaves()) {
            VoteChunk chunk = new VoteChunk(N);
            chunkService.create().create(computeService.getStatusMaster().getConnectedSlaves().get(k),chunk);
            chunkService.put().put(chunk);
            voteChunks[k] = chunk;
            k++;
        }
        //System.out.println("nid: " + bootService.getNodeID() + " VERTEX COUNT: " + N);

        RunLumpPrRoundTask Run1 = new RunLumpPrRoundTask(N,DAMPING_FACTOR,0,false);
        RunLumpPrRoundTask Run2 = new RunLumpPrRoundTask(N,DAMPING_FACTOR,1,false);

        //TaskScript taskScript = new TaskScript(Run1,Run2);

        TaskScript taskScriptRun1 = new TaskScript(Run1);
        TaskScript taskScriptRun2 = new TaskScript(Run2);

        ArrayList<Double> roundPRsum = new ArrayList<>();
        ArrayList<Double> roundPRerr = new ArrayList<>();

        int NumRounds = 0;
        double danglingPR;
        double PRerr = 0.0;
        ArrayList<Long> iterationTimes = new ArrayList<>();
        TaskScriptState state;
        for (int i = 0; i < MAX_ROUNDS; i++) {
            danglingPR = 1;
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
            chunkService.get().get(voteChunks);
            for (VoteChunk voteChunk : voteChunks) {
                PRerr += voteChunk.getPRerr();
                danglingPR -= voteChunk.getPRsum();
            }
            for (VoteChunk voteChunk : voteChunks) {
                voteChunk.setPRsum(danglingPR);
            }
            chunkService.put().put(voteChunks);
            System.out.println(danglingPR);
            /*chunkService.get().get(voteChunk,ChunkLockOperation.WRITE_LOCK_ACQ_PRE_OP);
            PRerr = voteChunk.getPRerr();
            danglingPR = 1 - voteChunk.getPRsum(Math.abs(i % 2 - 1));
            voteChunk.resetSum(i % 2, danglingPR);
            voteChunk.resetErr();
            chunkService.put().put(voteChunk,ChunkLockOperation.WRITE_LOCK_REL_POST_OP);*/
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

        RunLumpPrRoundTask calcDanglingPR = new RunLumpPrRoundTask(N,DAMPING_FACTOR,NumRounds % 2,true);
        TaskScript taskScriptCalcDanglingPR = new TaskScript(calcDanglingPR);
        state = computeService.submitTaskScript(taskScriptCalcDanglingPR,(short) 0);
        while (!state.hasTaskCompleted()) {
            try {
                Thread.sleep(100);
            } catch (final InterruptedException ignore) {

            }
        }






        /*RunPrRoundTask Run1 = new RunPrRoundTask(N,DAMPING_FACTOR,0,voteChunk.getID());
        RunPrRoundTask Run2 = new RunPrRoundTask(N,DAMPING_FACTOR,1,voteChunk.getID());

        //TaskScript taskScript = new TaskScript(Run1,Run2);

        TaskScript taskScriptRun1 = new TaskScript(Run1);
        TaskScript taskScriptRun2 = new TaskScript(Run2);

        ArrayList<Double> roundPRsum = new ArrayList<>();
        ArrayList<Double> roundPRerr = new ArrayList<>();

        int NumRounds = 0;
        //double PRsum = 0.0;
        double PRerr = 0.0;
        stopwatch.start();
        TaskScriptState state;
        for (int i = 0; i < 30; i++) {
            if(i % 2 == 0){
                state = computeService.submitTaskScript(taskScriptRun1, (short) 0, listener);
            } else {
                state = computeService.submitTaskScript(taskScriptRun2, (short) 0, listener);
            }
            while (!state.hasTaskCompleted()) {
                try {
                    Thread.sleep(100);
                } catch (final InterruptedException ignore) {

                }
            }

            chunkService.get().get(voteChunk,ChunkLockOperation.WRITE_LOCK_ACQ_PRE_OP);
            PRerr = voteChunk.getPRerr();
            //PRsum = voteChunk.getPRsum();
            voteChunk.reset();
            chunkService.put().put(voteChunk,ChunkLockOperation.WRITE_LOCK_REL_POST_OP);
            //System.out.println("Err: " + PRerr + " Sum: " + PRsum);
            roundPRerr.add(PRerr);
            //roundPRsum.add(PRsum);
            NumRounds++;

            if (PRerr <= 1e-4) {
                break;
            }
        }*/
        //System.out.println("Timer Computation: " + stopwatch.getTimeStr());
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


        //double[] roundPRsumArr = roundPRsum.stream().mapToDouble(i -> i).toArray();
        double[] roundPRerrArr = roundPRerr.stream().mapToDouble(i -> i).toArray();
        long[] iterationTimesArr = iterationTimes.stream().mapToLong(i -> i).toArray();

        ArrayList<Short> slaves = computeService.getStatusMaster((short)0).getConnectedSlaves();
        double memUsage = 0.0;
        for (short slave : slaves){
            memUsage += chunkService.status().getStatus(slave).getHeapStatus().getUsedSize().getMBDouble();
        }

        chunkService.get().get(edgeCnt);
        //System.out.println("EdgeCnt:" + edgeCnt.get_value());
        PrStatisticsJob prStatisticsJob = new PrStatisticsJob(outDir,filename,N,edgeCnt.get_value(),DAMPING_FACTOR,THRESHOLD,inputTime,iterationTimesArr,memUsage,roundPRerrArr,locality,meanInDeg);
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




    @Override
    public void signalShutdown() {
        // Interrupt any flow of your application and make sure it shuts down.
        // Do not block here or wait for something to shut down. Shutting down of your application
        // must be execute asynchronously
    }
}
