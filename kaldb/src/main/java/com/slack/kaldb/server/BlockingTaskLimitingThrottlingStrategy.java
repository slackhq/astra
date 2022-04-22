/*
 * Copyright 2022 LINE Corporation
 *
 * LINE Corporation licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.slack.kaldb.server;

import static com.google.common.base.Preconditions.checkArgument;

import com.linecorp.armeria.common.Request;
import com.linecorp.armeria.common.annotation.Nullable;
import com.linecorp.armeria.common.util.BlockingTaskExecutor;
import com.linecorp.armeria.common.util.SettableIntSupplier;
import com.linecorp.armeria.server.ServiceRequestContext;
import com.linecorp.armeria.server.throttling.ThrottlingStrategy;
import java.lang.reflect.Field;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link ThrottlingStrategy} that provides a throttling strategy based on the queue size of the
 * {@link BlockingTaskExecutor}.
 *
 * <p>Note that the size of the queue and the limit of the {@link SettableIntSupplier} are not
 * guaranteed to be atomic.
 *
 * <p><a
 * href="https://github.com/line/armeria/pull/4073">https://github.com/line/armeria/pull/4073</a>
 */
public final class BlockingTaskLimitingThrottlingStrategy<T extends Request>
    extends ThrottlingStrategy<T> {
  private static final Logger logger =
      LoggerFactory.getLogger(BlockingTaskLimitingThrottlingStrategy.class);

  private final SettableIntSupplier settableLimitSupplier;

  BlockingTaskLimitingThrottlingStrategy(
      SettableIntSupplier settableLimitSupplier, @Nullable String name) {
    super(name);
    checkArgument(settableLimitSupplier.getAsInt() > 0, "limit must be larger than zero");
    this.settableLimitSupplier = settableLimitSupplier;
  }

  @Override
  public CompletionStage<Boolean> accept(ServiceRequestContext ctx, T request) {

    ExecutorService executorService = ctx.blockingTaskExecutor().withoutContext();
    if (executorService instanceof BlockingTaskExecutor) {
      executorService = ((BlockingTaskExecutor) executorService).unwrap();
    }

    final ThreadPoolExecutor executor =
        unwrapThreadPoolExecutor(
            executorService, executorService.getClass().getSuperclass().getSuperclass());

    if (executor.getQueue().size() < settableLimitSupplier.getAsInt()) {
      return CompletableFuture.completedFuture(true);
    } else {
      return CompletableFuture.completedFuture(false);
    }
  }

  private ThreadPoolExecutor unwrapThreadPoolExecutor(
      ExecutorService executorService, Class<?> wrapper) {
    try {
      final Field executor = wrapper.getDeclaredField("executor");
      executor.setAccessible(true);
      final BlockingTaskExecutor blockingTaskExecutor =
          (BlockingTaskExecutor) executor.get(executorService);
      final Field delegate = blockingTaskExecutor.getClass().getDeclaredField("delegate");
      delegate.setAccessible(true);
      return (ThreadPoolExecutor) delegate.get(blockingTaskExecutor);
    } catch (NoSuchFieldException | IllegalAccessException | RuntimeException e) {
      logger.info("Cannot unwrap ThreadPoolExecutor", e);
      throw new IllegalStateException("Cannot throttle unwrap ThreadPoolExecutor", e);
    }
  }
}
