/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexing.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import com.metamx.common.Pair;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.emitter.EmittingLogger;
import io.druid.concurrent.Execs;
import io.druid.indexing.common.TaskLocation;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.TaskRunner;
import io.druid.indexing.overlord.TaskRunnerListener;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * The monitor watches ZK at a specified path for new tasks to appear. Upon starting the monitor, a listener will be
 * created that waits for new tasks. Tasks are executed as soon as they are seen.
 */
public class WorkerTaskMonitor
{
  private static final EmittingLogger log = new EmittingLogger(WorkerTaskMonitor.class);

  private final ObjectMapper jsonMapper;
  private final PathChildrenCache pathChildrenCache;
  private final CuratorFramework cf;
  private final WorkerCuratorCoordinator workerCuratorCoordinator;
  private final TaskRunner taskRunner;
  private final ExecutorService exec;

  private final BlockingQueue<Notice> notices = new LinkedBlockingDeque<>();
  private final Map<String, TaskDetails> running = new ConcurrentHashMap<>();

  private final Object lifecycleLock = new Object();
  private volatile boolean started = false;

  @Inject
  public WorkerTaskMonitor(
      ObjectMapper jsonMapper,
      CuratorFramework cf,
      WorkerCuratorCoordinator workerCuratorCoordinator,
      TaskRunner taskRunner
  )
  {
    this.jsonMapper = jsonMapper;
    this.pathChildrenCache = new PathChildrenCache(
        cf, workerCuratorCoordinator.getTaskPathForWorker(), false, true, Execs.makeThreadFactory("TaskMonitorCache-%s")
    );
    this.cf = cf;
    this.workerCuratorCoordinator = workerCuratorCoordinator;
    this.taskRunner = taskRunner;
    this.exec = Execs.singleThreaded("WorkerTaskMonitor");
  }

  /**
   * Register a monitor for new tasks. When new tasks appear, the worker node announces a status to indicate it has
   * started the task. When the task is complete, the worker node updates the status.
   */
  @LifecycleStart
  public void start()
  {
    synchronized (lifecycleLock) {
      Preconditions.checkState(!started, "already started");
      Preconditions.checkState(!exec.isShutdown(), "already stopped");
      started = true;

      try {
        restoreRestorableTasks();
        cleanupStaleAnnouncements();
        registerRunListener();
        registerLocationListener();
        pathChildrenCache.start();
        exec.submit(
            new Runnable()
            {
              @Override
              public void run()
              {
                mainLoop();
              }
            }
        );

        log.info("Started WorkerTaskMonitor.");
        started = true;
      }
      catch (Exception e) {
        log.makeAlert(e, "Exception starting WorkerTaskMonitor")
           .emit();
        throw Throwables.propagate(e);
      }
    }
  }

  private void mainLoop()
  {
    try {
      while (!Thread.currentThread().isInterrupted()) {
        final Notice notice = notices.take();

        try {
          notice.handle();
        }
        catch (Exception e) {
          log.makeAlert(e, "Failed to handle notice")
             .addData("noticeClass", notice.getClass().getSimpleName())
             .addData("noticeTaskId", notice.getTaskId())
             .emit();
        }
      }
    }
    catch (InterruptedException e) {
      log.info("WorkerTaskMonitor interrupted, exiting.");
    }
  }

  private void restoreRestorableTasks()
  {
    final List<Pair<Task, ListenableFuture<TaskStatus>>> restored = taskRunner.restore();
    for (Pair<Task, ListenableFuture<TaskStatus>> pair : restored) {
      addRunningTask(pair.lhs, pair.rhs);
    }
  }

  private void cleanupStaleAnnouncements()
  {
    // cleanup any old running task announcements which are invalid after restart
    for (TaskAnnouncement announcement : workerCuratorCoordinator.getAnnouncements()) {
      if (!running.containsKey(announcement.getTaskStatus().getId()) && announcement.getTaskStatus().isRunnable()) {
        log.info("Cleaning up stale announcement for task [%s].", announcement.getTaskStatus().getId());
        workerCuratorCoordinator.updateTaskStatusAnnouncement(
            TaskAnnouncement.create(
                announcement.getTaskStatus().getId(),
                announcement.getTaskResource(),
                TaskStatus.failure(announcement.getTaskStatus().getId()),
                TaskLocation.unknown()
            )
        );
      }
    }
  }

  private void registerRunListener()
  {
    pathChildrenCache.getListenable().addListener(
        new PathChildrenCacheListener()
        {
          @Override
          public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent pathChildrenCacheEvent)
              throws Exception
          {
            if (pathChildrenCacheEvent.getType().equals(PathChildrenCacheEvent.Type.CHILD_ADDED)) {
              final Task task = jsonMapper.readValue(
                  cf.getData().forPath(pathChildrenCacheEvent.getData().getPath()),
                  Task.class
              );

              notices.add(new RunNotice(task));
            }
          }
        }
    );
  }

  private void registerLocationListener()
  {
    taskRunner.registerListener(
        new TaskRunnerListener()
        {
          @Override
          public void locationChanged(final String taskId, final TaskLocation newLocation)
          {
            notices.add(new LocationNotice(taskId, newLocation));
          }
        },
        MoreExecutors.sameThreadExecutor()
    );
  }

  private void addRunningTask(final Task task, final ListenableFuture<TaskStatus> future)
  {
    running.put(task.getId(), new TaskDetails(task));
    Futures.addCallback(
        future,
        new FutureCallback<TaskStatus>()
        {
          @Override
          public void onSuccess(TaskStatus result)
          {
            notices.add(new StatusNotice(task, result));
          }

          @Override
          public void onFailure(Throwable t)
          {
            notices.add(new StatusNotice(task, TaskStatus.failure(task.getId())));
          }
        }
    );
  }

  @LifecycleStop
  public void stop()
  {
    synchronized (lifecycleLock) {
      Preconditions.checkState(started, "not started");

      try {
        exec.shutdownNow();
        pathChildrenCache.close();
        taskRunner.stop();

        started = false;
        log.info("Stopped WorkerTaskMonitor.");
      }
      catch (Exception e) {
        log.makeAlert(e, "Exception stopping WorkerTaskMonitor")
           .emit();
      }
    }
  }

  private static class TaskDetails
  {
    private final Task task;
    private final long startTime;
    private TaskStatus status;
    private TaskLocation location;

    public TaskDetails(Task task)
    {
      this.task = task;
      this.startTime = System.currentTimeMillis();
      this.status = TaskStatus.running(task.getId());
      this.location = TaskLocation.unknown();
    }
  }

  private interface Notice
  {
    String getTaskId();

    void handle();
  }

  private class RunNotice implements Notice
  {
    private final Task task;

    public RunNotice(Task task)
    {
      this.task = task;
    }

    @Override
    public String getTaskId()
    {
      return task.getId();
    }

    @Override
    public void handle()
    {
      if (running.containsKey(task.getId())) {
        log.warn(
            "Got run notice for task [%s] that I am already running...",
            task.getId()
        );
        workerCuratorCoordinator.removeTaskRunZnode(task.getId());
        return;
      }

      log.info("Submitting runnable for task[%s]", task.getId());

      workerCuratorCoordinator.updateTaskStatusAnnouncement(
          TaskAnnouncement.create(
              task,
              TaskStatus.running(task.getId()),
              TaskLocation.unknown()
          )
      );

      log.info("Affirmative. Running task [%s]", task.getId());
      workerCuratorCoordinator.removeTaskRunZnode(task.getId());
      final ListenableFuture<TaskStatus> future = taskRunner.run(task);
      addRunningTask(task, future);
    }
  }

  private class StatusNotice implements Notice
  {
    private final Task task;
    private final TaskStatus status;

    public StatusNotice(Task task, TaskStatus status)
    {
      this.task = task;
      this.status = status;
    }

    @Override
    public String getTaskId()
    {
      return task.getId();
    }

    @Override
    public void handle()
    {
      final TaskDetails details = running.get(task.getId());

      if (details == null) {
        log.warn("Got status notice for task [%s] that isn't running...", task.getId());
        return;
      }

      if (!status.isComplete()) {
        log.warn(
            "WTF?! Got status notice for task [%s] that isn't complete (status = [%s])...",
            task.getId(),
            status.getStatusCode()
        );
        return;
      }

      details.status = status.withDuration(System.currentTimeMillis() - details.startTime);

      try {
        workerCuratorCoordinator.updateTaskStatusAnnouncement(
            TaskAnnouncement.create(
                details.task,
                details.status,
                details.location
            )
        );
        log.info(
            "Job's finished. Completed [%s] with status [%s]",
            task.getId(),
            status.getStatusCode()
        );
      }
      catch (Exception e) {
        log.makeAlert(e, "Failed to update task announcement")
           .addData("task", task.getId())
           .emit();
      }
      finally {
        running.remove(task.getId());
      }
    }
  }

  private class LocationNotice implements Notice
  {
    private final String taskId;
    private final TaskLocation location;

    public LocationNotice(String taskId, TaskLocation location)
    {
      this.taskId = taskId;
      this.location = location;
    }

    @Override
    public String getTaskId()
    {
      return taskId;
    }

    @Override
    public void handle()
    {
      final TaskDetails details = running.get(taskId);

      if (details == null) {
        log.warn("Got location notice for task [%s] that isn't running...", taskId);
        return;
      }

      if (!Objects.equals(details.location, location)) {
        details.location = location;

        try {
          log.info("Updating task [%s] announcement with location [%s]", taskId, location);
          workerCuratorCoordinator.updateTaskStatusAnnouncement(
              TaskAnnouncement.create(
                  details.task,
                  details.status,
                  details.location
              )
          );
        }
        catch (Exception e) {
          log.makeAlert(e, "Failed to update task announcement")
             .addData("task", taskId)
             .emit();
        }
      }
    }
  }
}
