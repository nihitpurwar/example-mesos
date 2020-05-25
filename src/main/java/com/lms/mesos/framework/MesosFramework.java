package com.lms.mesos.framework;

import com.lms.mesos.scheduler.MyMesosScheduler;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.CommandInfo;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.FrameworkInfo;


public class MesosFramework {
  // TODO: replace this
  private static final String JAR_WITH_DEPENDENCIES =
      "/Users/npurwar/work/example-mesos/target/example-mesos-1.0-SNAPSHOT-jar-with-dependencies.jar";

  public static void main(String[] args) throws InterruptedException {
    CommandInfo.URI uri = CommandInfo.URI.newBuilder().setValue(JAR_WITH_DEPENDENCIES).setExtract(false).build();
    String command = "java -cp " + JAR_WITH_DEPENDENCIES + " com.lms.mesos.executor.MyMesosExecutor";
    CommandInfo commandInfo = CommandInfo.newBuilder().setValue(command).addUris(uri).build();

    ExecutorInfo executorInfo = ExecutorInfo.newBuilder()
        .setExecutorId(Protos.ExecutorID.newBuilder().setValue("MyMesosExecutor"))
        .setCommand(commandInfo)
        .setName("My New Scheduler Executor")
        .setSource("java")
        .build();

    FrameworkInfo.Builder frameworkBuilder =
        FrameworkInfo.newBuilder().setFailoverTimeout(120000).setUser("").setName("My New Scheduler Framework");
    frameworkBuilder.setPrincipal("new-scheduler-framework");

    MesosSchedulerDriver driver =
        new MesosSchedulerDriver(new MyMesosScheduler(executorInfo), frameworkBuilder.build(), "127.0.0.1:5050");

    int status = driver.run() == Protos.Status.DRIVER_STOPPED ? 0 : 1;

    System.err.println("Stopping driver in 3 min");
    Thread.sleep(180 * 1000);

    driver.stop();
    System.exit(status);
  }
}
