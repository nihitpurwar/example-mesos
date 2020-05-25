package com.lms.mesos.scheduler;

import java.util.ArrayList;
import java.util.List;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

import static org.apache.mesos.Protos.Value.Type.*;


public class MyMesosScheduler implements Scheduler {
  private Protos.ExecutorInfo executor;
  private int launchedTasks = 0;

  public MyMesosScheduler(Protos.ExecutorInfo executor) {
    this.executor = executor;
  }

  public void registered(SchedulerDriver schedulerDriver, Protos.FrameworkID frameworkID,
      Protos.MasterInfo masterInfo) {
    System.err.println("registered");
  }

  public void reregistered(SchedulerDriver schedulerDriver, Protos.MasterInfo masterInfo) {
    System.err.println("re-registered");
  }

  public void resourceOffers(SchedulerDriver schedulerDriver, List<Protos.Offer> list) {
    System.err.println("resourceOffers");

    List<Protos.OfferID> offerIDS = new ArrayList<>();
    List<Protos.TaskInfo> tasks = new ArrayList<Protos.TaskInfo>();

    for (Protos.Offer offer : list) {
      Protos.TaskID taskId = Protos.TaskID.newBuilder().setValue(Integer.toString(launchedTasks++)).build();

      for (Protos.Resource resource : offer.getResourcesList()) {
        System.err.println("offer res = " + resource.getName() + " details " + resource.getScalar().getValue());
      }

      Protos.ExecutorInfo executor = getExecutorFromOffer(offer);
      if (executor != null) {

        Protos.Value.Scalar.Builder cpuValue = Protos.Value.Scalar.newBuilder().setValue(1);
        Protos.TaskInfo task = Protos.TaskInfo.newBuilder()
            .setName("Task " + taskId.getValue())
            .setTaskId(taskId)
            .setSlaveId(offer.getSlaveId())
            .addResources(Protos.Resource.newBuilder().setName("cpus").setType(SCALAR).setScalar(cpuValue))
            .setExecutor(Protos.ExecutorInfo.newBuilder(executor))
            .build();
        offerIDS.add(offer.getId());
        tasks.add(task);
      } else {
        // decline offer if you do not have any task to do
        schedulerDriver.declineOffer(offer.getId());
      }
    }
    schedulerDriver.launchTasks(offerIDS, tasks);
  }

  private Protos.ExecutorInfo getExecutorFromOffer(Protos.Offer offer) {
    // TODO: can read from a db, etc to find the matching task
    return executor;
  }

  public void offerRescinded(SchedulerDriver schedulerDriver, Protos.OfferID offerID) {
    System.err.println("offerRescinded");
  }

  public void statusUpdate(SchedulerDriver schedulerDriver, Protos.TaskStatus taskStatus) {
    System.err.println("statusUpdate " + taskStatus.getMessage());
  }

  public void frameworkMessage(SchedulerDriver schedulerDriver, Protos.ExecutorID executorID, Protos.SlaveID slaveID,
      byte[] bytes) {
    System.err.println("frameworkMessage");
  }

  public void disconnected(SchedulerDriver schedulerDriver) {
    System.err.println("disconnected");
  }

  public void slaveLost(SchedulerDriver schedulerDriver, Protos.SlaveID slaveID) {
    System.err.println("slaveLost");
  }

  public void executorLost(SchedulerDriver schedulerDriver, Protos.ExecutorID executorID, Protos.SlaveID slaveID,
      int i) {
    System.err.println("executorLost");
  }

  public void error(SchedulerDriver schedulerDriver, String s) {
    System.err.println("error " + s);
  }
}
