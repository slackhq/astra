package com.slack.kaldb.chunkManager;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Provides a wrapping executor service which cannot be requested to be shutdown. This is intended
 * to be used when passing an executor service between classes, to ensure an instantiated class
 * cannot request the shutdown of the parent class's executor service.
 */
public class ProtectedExecutorService implements ExecutorService {

  private final ExecutorService proxiedExecutorService;

  public ProtectedExecutorService(ExecutorService proxiedExecutorService) {
    this.proxiedExecutorService = proxiedExecutorService;
  }

  @Override
  @Deprecated
  public void shutdown() {
    throw new IllegalStateException(
        "ProtectedExecutorService cannot be shutdown from this context");
  }

  @Override
  @Deprecated
  public List<Runnable> shutdownNow() {
    throw new IllegalStateException(
        "ProtectedExecutorService cannot be shutdown from this context");
  }

  @Override
  public boolean isShutdown() {
    return proxiedExecutorService.isShutdown();
  }

  @Override
  public boolean isTerminated() {
    return proxiedExecutorService.isTerminated();
  }

  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    return proxiedExecutorService.awaitTermination(timeout, unit);
  }

  @Override
  public <T> Future<T> submit(Callable<T> task) {
    return proxiedExecutorService.submit(task);
  }

  @Override
  public <T> Future<T> submit(Runnable task, T result) {
    return proxiedExecutorService.submit(task, result);
  }

  @Override
  public Future<?> submit(Runnable task) {
    return proxiedExecutorService.submit(task);
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
      throws InterruptedException {
    return proxiedExecutorService.invokeAll(tasks);
  }

  @Override
  public <T> List<Future<T>> invokeAll(
      Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
      throws InterruptedException {
    return proxiedExecutorService.invokeAll(tasks, timeout, unit);
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
      throws InterruptedException, ExecutionException {
    return proxiedExecutorService.invokeAny(tasks);
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    return proxiedExecutorService.invokeAny(tasks, timeout, unit);
  }

  @Override
  public void execute(Runnable command) {
    proxiedExecutorService.execute(command);
  }

  public static ProtectedExecutorService wrap(ExecutorService executorService) {
    return new ProtectedExecutorService(executorService);
  }
}
