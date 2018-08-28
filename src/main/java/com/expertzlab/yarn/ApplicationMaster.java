package com.expertzlab.yarn;

import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;

import java.io.IOException;
import java.util.Collections;

/**
 * Created by admin on 29/08/18.
 */
public class ApplicationMaster {

    public static void main(String[] args) throws IOException, YarnException, InterruptedException {

        System.out.println("Rnning AM");
        String shellCmd = args[0];
        int numContainers = Integer.valueOf(args[1]);
        YarnConfiguration conf = new YarnConfiguration();

        System.out.println("Initializeing AM RM Client");
        AMRMClient<AMRMClient.ContainerRequest> rmClient = AMRMClient.createAMRMClient();
        rmClient.init(conf);
        rmClient.start();

        System.out.println("Initializing NM client");
        NMClient nmClient = NMClient.createNMClient();
        nmClient.init(conf);
        nmClient.start();

        System.out.println("Register Application Master");
        rmClient.registerApplicationMaster("localhost",0,"");

        Priority priority = Records.newRecord(Priority.class);
        priority.setPriority(0);

        System.out.println("Setting Resource capability for containers");
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(128);
        capability.setVirtualCores(1);
        for(int i = 0; i < numContainers; ++i){
            AMRMClient.ContainerRequest containerRequested = new AMRMClient.ContainerRequest(
                    capability, null, null, priority, true
            );
            rmClient.addContainerRequest(containerRequested);
        }

        int allocatedContainers = 0;
        System.out.println("Requesting container allocation from resource Manager");
        while(allocatedContainers < numContainers){
            AllocateResponse response = rmClient.allocate(0);
            for(Container container: response.getAllocatedContainers()){
                ++allocatedContainers;
                //Launch container by creating Container Launch Context
                ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
                ctx.setCommands(Collections.singletonList(shellCmd +
                " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR+"/stdout"+
                " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR+"/stderr"));

                System.out.println("Starting container on node:"+
                    container.getNodeHttpAddress());
                nmClient.startContainer(container,ctx);
            }
            Thread.sleep(100);
        }

        int completedContainers = 0;
        while(completedContainers < numContainers){
            AllocateResponse response = rmClient.allocate(completedContainers/numContainers);
            for(ContainerStatus status: response.getCompletedContainersStatuses()){
                ++completedContainers;
                System.out.println("Container Completed:" + status.getContainerId());
                System.out.println("Num completed containers:"+completedContainers);
            }
            Thread.sleep(100);
        }

        rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED,"","");
    }
}
