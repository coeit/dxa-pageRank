package de.hhu.bsinfo.dxapp;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import de.hhu.bsinfo.dxmem.core.CIDTable;
import de.hhu.bsinfo.dxmem.core.CIDTableChunkEntry;
import de.hhu.bsinfo.dxmem.data.ChunkByteArray;
import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxmem.data.ChunkID;
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
        BootService bootService = getService(BootService.class);
        ChunkService chunkService = getService(ChunkService.class);
        NameserviceService nameService = getService(NameserviceService.class);
        MasterSlaveComputeService computeService = getService(MasterSlaveComputeService.class);
        JobService jobService = getService(JobService.class);
        ApplicationService applicationService = getService(ApplicationService.class);
        FunctionService functionService = getService(FunctionService.class);

        short input_nid = computeService.getStatusMaster((short) 0).getConnectedSlaves().get(0);

        Stopwatch stopwatch = new Stopwatch();

        InputJob inputJob = new InputJob(p_args[0]);
        stopwatch.start();
        jobService.pushJobRemote(inputJob, computeService.getStatusMaster((short) 0).getConnectedSlaves().get(0));
        jobService.waitForAllJobsToFinish();
        stopwatch.stop();
        System.out.println("Timer InputJob: " + stopwatch.getTimeStr());
        for (short nodeID : computeService.getStatusMaster((short) 0).getConnectedSlaves()) {
            IntegerChunk chunk = new IntegerChunk();
            chunkService.create().create(bootService.getNodeID(),chunk);
            nameService.register(chunk,NodeID.toHexString(nodeID).substring(2,6));
        }

        IntegerChunk vCnt = new IntegerChunk(nameService.getChunkID("vCnt",333));
        chunkService.get().get(vCnt);
        int N = vCnt.get_value();
        System.out.println("nid: " + bootService.getNodeID() + " VERTEX COUNT: " + N);

        TaskListener listener = new TaskListener() {
            @Override
            public void taskBeforeExecution(final TaskScriptState p_taskScriptState) {
                System.out.println("ComputeTask: Starting execution");
            }

            @Override
            public void taskCompleted(final TaskScriptState p_taskScriptState) {
                System.out.println("ComputeTask: Finished execution ");
            }
        };

        PRInfoTask PRInfo = new PRInfoTask();
        SendPrTask SendPRpar = new SendPrTask(N,DAMPING_FACTOR);
        UpdatePrTask updatePR = new UpdatePrTask();

        RunPrRoundTask Run1 = new RunPrRoundTask(N,DAMPING_FACTOR,false);
        RunPrRoundTask Run2 = new RunPrRoundTask(N,DAMPING_FACTOR,true);


        //TaskScript taskScriptSend = new TaskScript(SendPRpar);
        //TaskScript taskScriptUpdate = new TaskScript(updatePR);

        //TaskScript taskScript = new TaskScript(SendPRpar,updatePR);
        TaskScript taskScript = new TaskScript(Run1,Run2);
        //TaskScript taskScript = new TaskScript(PRInfo,SendPRpar,updatePR,PRInfo,SendPRpar,updatePR,PRInfo,SendPRpar,updatePR,PRInfo);
        //TaskScript taskScript = new TaskScript(SendPR,updatePR,SendPR,updatePR,SendPR,updatePR,PRInfo);
        // TaskScript taskScript = new TaskScript(PRInfo,SendPRpar,updatePR,SendPRpar,updatePR,SendPRpar,updatePR,SendPRpar,updatePR,SendPRpar,updatePR,PRInfo);
        stopwatch.start();
        for (int i = 0; i < 10; i++) {
            //int votes = 0;
            TaskScriptState state = computeService.submitTaskScript(taskScript, (short) 0, listener);
            while (!state.hasTaskCompleted() && computeService.getStatusMaster((short) 0).getNumTasksQueued() != 0) {
                try {
                    Thread.sleep(100);
                } catch (final InterruptedException ignore) {

                }
            }
            /*TaskScriptState state1 = computeService.submitTaskScript(taskScriptUpdate,(short) 0,listener);
            while (!state1.hasTaskCompleted() && computeService.getStatusMaster((short) 0).getNumTasksQueued() != 0) {
                try {
                    Thread.sleep(100);
                } catch (final InterruptedException ignore) {

                }
            }*/
            /*for (short nodeID: computeService.getStatusMaster((short) 0).getConnectedSlaves()){
                IntegerChunk integerChunk = new IntegerChunk(nameService.getChunkID(NodeID.toHexString(nodeID).substring(2,6),333));
                chunkService.get().get(integerChunk);
                System.out.println(NodeID.toHexString(nodeID) + " votes: " + integerChunk.get_value());
                votes += integerChunk.get_value();
            }
            if((double) votes / (double) N >= 0.8){
                System.out.println(">>Reached vote halting limit in round " + i);
                break;
            }*/
        }
        stopwatch.stop();
        System.out.println("Timer Computation: " + stopwatch.getTimeStr());
	    TaskScript PRInfoTaskScript = new TaskScript(PRInfo);
	    TaskScriptState PRInfoTaskScriptState = computeService.submitTaskScript(PRInfoTaskScript, (short) 0, listener);
        while (!PRInfoTaskScriptState.hasTaskCompleted() && computeService.getStatusMaster((short) 0).getNumTasksQueued() != 0) {
            try {
                Thread.sleep(100);
            } catch (final InterruptedException ignore) {

            }
        }
    }


    @Override
    public void signalShutdown() {
        // Interrupt any flow of your application and make sure it shuts down.
        // Do not block here or wait for something to shut down. Shutting down of your application
        // must be execute asynchronously
    }
}
