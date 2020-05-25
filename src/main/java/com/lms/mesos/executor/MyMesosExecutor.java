package com.lms.mesos.executor;

import java.io.File;
import java.io.FileWriter;
import java.util.Date;
import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.MesosExecutorDriver;
import org.apache.mesos.Protos;


public class MyMesosExecutor implements Executor {

  private static final String FILE_NAME = "/Users/npurwar/tmp/test-mesos.txt";

  @Override
  public void registered(ExecutorDriver executorDriver, Protos.ExecutorInfo executorInfo,
      Protos.FrameworkInfo frameworkInfo, Protos.SlaveInfo slaveInfo) {
    System.err.println("MyMesosExecutor registered");
  }

  @Override
  public void reregistered(ExecutorDriver executorDriver, Protos.SlaveInfo slaveInfo) {
    System.err.println("MyMesosExecutor reregistered");
  }

  @Override
  public void disconnected(ExecutorDriver executorDriver) {
    System.err.println("MyMesosExecutor disconnected");
  }

  @Override
  public void launchTask(ExecutorDriver driver, Protos.TaskInfo task) {
    Protos.TaskStatus status =
        Protos.TaskStatus.newBuilder().setTaskId(task.getTaskId()).setState(Protos.TaskState.TASK_RUNNING).build();
    driver.sendStatusUpdate(status);

    String workStatement = "Mesos agent completed work at " + new Date();
    System.err.println(workStatement);

    try {
      File file = new File(FILE_NAME);
      //noinspection ResultOfMethodCallIgnored
      file.createNewFile();
      FileWriter fileWriter = new FileWriter(FILE_NAME);
      fileWriter.write(workStatement + "\n");
      fileWriter.close();
    } catch (Exception e) {
      System.err.println("Exception occurred " + e.getMessage());
      e.printStackTrace();
    }

    driver.sendFrameworkMessage(workStatement.getBytes());
    status =
        Protos.TaskStatus.newBuilder().setTaskId(task.getTaskId()).setState(Protos.TaskState.TASK_FINISHED).build();
    driver.sendStatusUpdate(status);
  }

  @Override
  public void killTask(ExecutorDriver executorDriver, Protos.TaskID taskID) {
    System.err.println("MyMesosExecutor killTask");
  }

  @Override
  public void frameworkMessage(ExecutorDriver executorDriver, byte[] bytes) {
    System.err.println("MyMesosExecutor frameworkMessage");
  }

  @Override
  public void shutdown(ExecutorDriver executorDriver) {
    System.err.println("MyMesosExecutor shutdown");
  }

  @Override
  public void error(ExecutorDriver executorDriver, String s) {
    System.err.println("MyMesosExecutor error " + s);
  }

  public static void main(String[] args) {
    MesosExecutorDriver driver = new MesosExecutorDriver(new MyMesosExecutor());
    System.exit(driver.run() == Protos.Status.DRIVER_STOPPED ? 0 : 1);
  }
}
