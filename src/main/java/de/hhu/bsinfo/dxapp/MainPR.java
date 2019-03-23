package de.hhu.bsinfo.dxapp;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import de.hhu.bsinfo.dxapp.chunk.VoteChunk;
import de.hhu.bsinfo.dxmem.core.CIDTable;
import de.hhu.bsinfo.dxmem.core.CIDTableChunkEntry;
import de.hhu.bsinfo.dxmem.data.ChunkByteArray;
import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxmem.data.ChunkLockOperation;
import de.hhu.bsinfo.dxram.app.AbstractApplication;
//import de.hhu.bsinfo.dxram.app.Application;
import de.hhu.bsinfo.dxram.app.ApplicationService;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.chunk.data.ChunkAnon;
import de.hhu.bsinfo.dxapp.chunk.IntegerChunk;
import de.hhu.bsinfo.dxram.engine.DXRAMVersion;
import de.hhu.bsinfo.dxram.function.*;
//import de.hhu.bsinfo.dxram.function.PRInputFunction;
import de.hhu.bsinfo.dxram.function.util.ParameterList;
import de.hhu.bsinfo.dxram.generated.BuildConfig;
import de.hhu.bsinfo.dxapp.jobs.*;
import de.hhu.bsinfo.dxram.job.*;
import de.hhu.bsinfo.dxram.logger.LoggerService;
import de.hhu.bsinfo.dxram.ms.*;
import de.hhu.bsinfo.dxram.ms.script.TaskScript;
import de.hhu.bsinfo.dxapp.tasks.*;
import de.hhu.bsinfo.dxutils.Stopwatch;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxram.ms.tasks.PrintTask;
import de.hhu.bsinfo.dxutils.NodeID;
import org.apache.logging.log4j.core.config.plugins.util.ResolverUtil;
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
        double DAMPING_FACTOR = 0.85;
        int N;
        if (p_args.length < 1){
            System.out.println("Not enough Arguments ... shutting down");
            signalShutdown();
        }




        BootService bootService = getService(BootService.class);
        ChunkService chunkService = getService(ChunkService.class);
        NameserviceService nameService = getService(NameserviceService.class);
        MasterSlaveComputeService computeService = getService(MasterSlaveComputeService.class);
        JobService jobService = getService(JobService.class);

        String outDir = createOutputDirs();


        IntegerChunk cntChunk = new IntegerChunk();
        chunkService.create().create(computeService.getStatusMaster((short) 0 ).getMasterNodeId(),cntChunk);
        chunkService.put().put(cntChunk);

        Stopwatch stopwatch = new Stopwatch();
        System.out.println("len: "  + p_args.length);
        if (p_args.length > 1) {
            N = Integer.parseInt(p_args[1]);
            File dir = new File(new File(p_args[0]).getParent());
            System.out.println(dir.getName());
            File[] files = dir.listFiles((d, name) -> name.contains(p_args[0] + "_split"));
            int i = 0;
            stopwatch.start();
            for (File file : files) {
                System.out.println(file.getName());
                InputPrDistJob inputPrDistJob = new InputPrDistJob(file.getName(),Integer.parseInt(p_args[1]));
                jobService.pushJobRemote(inputPrDistJob,computeService.getStatusMaster((short) 0).getConnectedSlaves().get(i));
                i++;
            }
        } else {
            chunkService.get().get(cntChunk);
            N = cntChunk.get_value();
            //System.out.println("nid: " + bootService.getNodeID() + " VERTEX COUNT: " + N);
            stopwatch.start();
            InputJob inputJob = new InputJob(p_args[0],cntChunk.getID());
            jobService.pushJobRemote(inputJob, computeService.getStatusMaster((short) 0).getConnectedSlaves().get(0));

        }
        jobService.waitForAllJobsToFinish();
        stopwatch.stop();
        //System.out.println("Timer InputJob: " + stopwatch.getTimeStr());
        long InputTime = stopwatch.getTime();
        VoteChunk voteChunk = new VoteChunk();
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
                System.out.println("ComputeTask: Starting execution on Node " + NodeID.toHexString(p_taskScriptState.getNodeIdSubmitted()));
            }

            @Override
            public void taskCompleted(final TaskScriptState p_taskScriptState) {
                System.out.println("ComputeTask: Finished execution on Node " + NodeID.toHexString(p_taskScriptState.getNodeIdSubmitted()));
            }
        };

        SendPrTask SendPRpar = new SendPrTask(N,DAMPING_FACTOR);
        UpdatePrTask updatePR = new UpdatePrTask();

        RunPrRoundTask Run1 = new RunPrRoundTask(N,DAMPING_FACTOR,false,voteChunk.getID());
        RunPrRoundTask Run2 = new RunPrRoundTask(N,DAMPING_FACTOR,true,voteChunk.getID());

        //TaskScript taskScript = new TaskScript(Run1,Run2);

        TaskScript taskScriptRun1 = new TaskScript(Run1);
        TaskScript taskScriptRun2 = new TaskScript(Run2);

        ArrayList<Integer> RoundVotes = new ArrayList<>();
        int NumRounds = 0;
        double PRsum = 0.0;
        int votes;
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
            votes = voteChunk.getVotes();
            PRsum = voteChunk.getPRsum();
            voteChunk.reset();
            chunkService.put().put(voteChunk,ChunkLockOperation.WRITE_LOCK_REL_POST_OP);
            /*for (short nodeID: computeService.getStatusMaster((short) 0).getConnectedSlaves()){
                VoteChunk voteChunk = new VoteChunk(nameService.getChunkID(NodeID.toHexString(nodeID).substring(2,6),333));
                chunkService.lock().lock(true,false,-1,voteChunk);
                chunkService.get().get(voteChunk);
                chunkService.lock().lock(false,false,-1,voteChunk);
                System.out.println(NodeID.toHexString(nodeID) + " votes Round " + i  + ": " + voteChunk.getVotes());
                votes += voteChunk.getVotes();
                PRsum += voteChunk.getPRsum();
            }*/
            RoundVotes.add(votes);
            NumRounds++;
            if((double) votes / (double) N >= 0.9){
                //System.out.println(">>Reached vote halting limit in round " + i);
                break;
            }


        }
        stopwatch.stop();
        //System.out.println("Timer Computation: " + stopwatch.getTimeStr());
        long ExecutionTime = stopwatch.getTime();
        PRInfoTask PRInfo = new PRInfoTask(outDir);
	    TaskScript PRInfoTaskScript = new TaskScript(PRInfo);
	    TaskScriptState PRInfoTaskScriptState = computeService.submitTaskScript(PRInfoTaskScript, (short) 0, listener);
        while (!PRInfoTaskScriptState.hasTaskCompleted() && computeService.getStatusMaster((short) 0).getNumTasksQueued() != 0) {
            try {
                Thread.sleep(100);
            } catch (final InterruptedException ignore) {

            }
        }

        int[] RoundVotesArr = RoundVotes.stream().mapToInt(i -> i).toArray();
        PrStatisticsJob prStatisticsJob = new PrStatisticsJob(outDir,N,InputTime,ExecutionTime,NumRounds,PRsum,RoundVotesArr);
        jobService.pushJobRemote(prStatisticsJob, computeService.getStatusMaster((short) 0).getConnectedSlaves().get(0));
        jobService.waitForAllJobsToFinish();

    }

    public String createOutputDirs(){
        String HOME = System.getProperty("user.home");
        File PrOutDir = new File(HOME + "/" + "dxa-pageRank_out");

        if (!PrOutDir.exists()){
            PrOutDir.mkdir();
        }
        String out = new SimpleDateFormat("yyyy-MM-dd_hh-mm-ss").format(new Date());
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
