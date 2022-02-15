package com.slack.kaldb.blobfs.s3;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.apache.lucene.store.RateLimiter;

public class RateLimitingDelegatingInputStream extends FilterInputStream {

  private final RateLimiter.SimpleRateLimiter simpleRateLimiter;
  private long bytesSinceLastRateLimit;

  public RateLimitingDelegatingInputStream(InputStream in) {
    super(in);
    simpleRateLimiter = new RateLimiter.SimpleRateLimiter(1);
  }

  private void maybePause(int bytes) {
    bytesSinceLastRateLimit += bytes;
    if (bytesSinceLastRateLimit >= simpleRateLimiter.getMinPauseCheckBytes()) {
      simpleRateLimiter.pause(bytesSinceLastRateLimit);
      bytesSinceLastRateLimit = 0;
    }
  }

  @Override
  public int read() throws IOException {
    int b = super.read();
    maybePause(1);
    return b;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    int n = super.read(b, off, len);
    if (n > 0) {
      maybePause(n);
    }
    return n;
  }
}
