package de.hhu.bsinfo.dxapp;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import de.hhu.bsinfo.dxapp.chunk.VoteChunk;
import de.hhu.bsinfo.dxmem.data.ChunkLockOperation;
import de.hhu.bsinfo.dxram.app.AbstractApplication;
//import de.hhu.bsinfo.dxram.app.Application;
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


        if (p_args.length < 4){
            System.out.println("Not enough Arguments ... shutting down");
            System.out.println("Arguments: graphfile vertexcnt dampingfactor errorthreshold (maxrounds:default=30)");
            signalShutdown();
        }
        String filename = p_args[0];
        int N = Integer.parseInt(p_args[1]);
        double DAMPING_FACTOR = Double.parseDouble(p_args[2]);
        double THRESHOLD = Double.parseDouble(p_args[3]);
        int MAX_ROUNDS = 30;

        if(p_args.length == 5){
            MAX_ROUNDS = Integer.parseInt(p_args[4]);
        }


        BootService bootService = getService(BootService.class);
        ChunkService chunkService = getService(ChunkService.class);
        NameserviceService nameService = getService(NameserviceService.class);
        MasterSlaveComputeService computeService = getService(MasterSlaveComputeService.class);
        JobService jobService = getService(JobService.class);



        IntegerChunk cntChunk = new IntegerChunk();
        chunkService.create().create(computeService.getStatusMaster((short) 0 ).getMasterNodeId(),cntChunk);
        chunkService.put().put(cntChunk);
        Stopwatch stopwatch = new Stopwatch();
        System.out.println("len: "  + p_args.length);
        /*if (p_args.length > 2) {
            File input_file = new File(filename);
            File dir = new File(input_file.getParentFile().getAbsolutePath());
            System.out.println(dir.getName());
            File[] files = dir.listFiles((d, name) -> name.contains(input_file.getName() + "_split"));
            StringBuilder builder = new StringBuilder();
            for (File file : files){
                    builder.append(file.getAbsolutePath() + "@");
            }
            InputPrDistTask inputPrDistTask = new InputPrDistTask(builder.toString(),N);
            TaskScript inputPrDistTaskScript = new TaskScript(inputPrDistTask);
            TaskScriptState inputPrDistTaskScriptState = computeService.submitTaskScript(inputPrDistTaskScript,(short) 0);
            stopwatch.start();
            while(!inputPrDistTaskScriptState.hasTaskCompleted()){
                try {
                    Thread.sleep(100);
                } catch (final InterruptedException ignore) {

                }
            }

        } else {
                        //System.out.println("nid: " + bootService.getNodeID() + " VERTEX COUNT: " + N);
            stopwatch.start();
            //InputJob inputJob = new InputJob(p_args[0],cntChunk.getID());
            InputPrJob inputPrJob = new InputPrJob(filename,N);
            jobService.pushJobRemote(inputPrJob, computeService.getStatusMaster((short) 0).getConnectedSlaves().get(0));
            jobService.waitForAllJobsToFinish();
        }*/

        /*ReadPartitionInEdgeListTask readPartitionInEdgeListTask = new ReadPartitionInEdgeListTask(filename,N);
        TaskScript inputTaskScript = new TaskScript(readPartitionInEdgeListTask);
        TaskScriptState inputState = computeService.submitTaskScript(inputTaskScript,(short) 0 );
        stopwatch.start();
        while(!inputState.hasTaskCompleted()){
            try {
                Thread.sleep(100);
            } catch (final InterruptedException ignore) {

            }
        }

        stopwatch.stop();*/

        ReadLumpInEdgeListTask readLumpInEdgeListTask = new ReadLumpInEdgeListTask(filename,N);
        TaskScript inputTaskScript = new TaskScript(readLumpInEdgeListTask);
        TaskScriptState inputState = computeService.submitTaskScript(inputTaskScript,(short) 0 );
        stopwatch.start();
        while(!inputState.hasTaskCompleted()){
            try {
                Thread.sleep(100);
            } catch (final InterruptedException ignore) {

            }
        }
        stopwatch.stop();
        //System.out.println("Timer InputJob: " + stopwatch.getTimeStr());
        long InputTime = stopwatch.getTime();
        VoteChunk voteChunk = new VoteChunk(N);
        chunkService.create().create(bootService.getNodeID(),voteChunk);
        chunkService.put().put(voteChunk);
        /*for (short nodeID : computeService.getStatusMaster((short) 0).getConnectedSlaves()) {
            VoteChunk chunk = new VoteChunk();
            chunkService.create().create(bootService.getNodeID(),chunk);
            nameService.register(chunk,NodeID.toHexString(nodeID).substring(2,6));
            chunkService.put().put(chunk);
        }*/
        System.out.println("nid: " + bootService.getNodeID() + " VERTEX COUNT: " + N);

        TaskListener listener = new TaskListener() {
            @Override
            public void taskBeforeExecution(final TaskScriptState p_taskScriptState) {
                System.out.println("ComputeTask: Starting execution");
            }

            @Override
            public void taskCompleted(final TaskScriptState p_taskScriptState) {
                System.out.println("ComputeTask: Finished execution");
            }
        };

        RunLumpPrRoundTask Run1 = new RunLumpPrRoundTask(N,DAMPING_FACTOR,voteChunk.getID(),0,false);
        RunLumpPrRoundTask Run2 = new RunLumpPrRoundTask(N,DAMPING_FACTOR,voteChunk.getID(),1,false);

        //TaskScript taskScript = new TaskScript(Run1,Run2);

        TaskScript taskScriptRun1 = new TaskScript(Run1);
        TaskScript taskScriptRun2 = new TaskScript(Run2);

        ArrayList<Double> roundPRsum = new ArrayList<>();
        ArrayList<Double> roundPRerr = new ArrayList<>();

        int NumRounds = 0;
        double danglingPR = 0.0;
        double PRerr = 0.0;
        ArrayList<Long> iterationTimes = new ArrayList<>();
        TaskScriptState state;
        for (int i = 0; i < MAX_ROUNDS; i++) {
            stopwatch.start();
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
            danglingPR = 1 - voteChunk.getPRsum(Math.abs(i % 2 - 1));
            voteChunk.resetSum(i % 2, danglingPR);
            voteChunk.resetErr();
            chunkService.put().put(voteChunk,ChunkLockOperation.WRITE_LOCK_REL_POST_OP);
            System.out.println("Sum: " + danglingPR);
            roundPRerr.add(PRerr);
            //roundPRsum.add(PRsum);
            NumRounds++;
            stopwatch.stop();
            iterationTimes.add(stopwatch.getTime());
            if (PRerr <= THRESHOLD) {
                break;
            }

        }

        RunLumpPrRoundTask calcDanglingPR = new RunLumpPrRoundTask(N,DAMPING_FACTOR,voteChunk.getID(),NumRounds % 2,true);
        TaskScript taskScriptCalcDanglingPR = new TaskScript(calcDanglingPR);
        state = computeService.submitTaskScript(taskScriptCalcDanglingPR,(short) 0, listener);
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

        PRInfoTask PRInfo = new PRInfoTask(outDir,NumRounds % 2);
	    TaskScript PRInfoTaskScript = new TaskScript(PRInfo);
	    TaskScriptState PRInfoTaskScriptState = computeService.submitTaskScript(PRInfoTaskScript, (short) 0, listener);
        while (!PRInfoTaskScriptState.hasTaskCompleted() && computeService.getStatusMaster((short) 0).getNumTasksQueued() != 0) {
            try {
                Thread.sleep(100);
            } catch (final InterruptedException ignore) {

            }
        }



        //double[] roundPRsumArr = roundPRsum.stream().mapToDouble(i -> i).toArray();
        double[] roundPRerrArr = roundPRerr.stream().mapToDouble(i -> i).toArray();
        long[] iterationTimesArr = iterationTimes.stream().mapToLong(i -> i).toArray();

        PrStatisticsJob prStatisticsJob = new PrStatisticsJob(outDir,N,DAMPING_FACTOR,THRESHOLD,InputTime,iterationTimesArr,NumRounds,roundPRerrArr);
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
