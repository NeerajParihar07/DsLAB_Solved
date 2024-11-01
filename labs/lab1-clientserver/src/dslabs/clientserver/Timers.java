package dslabs.clientserver;

import dslabs.framework.Timer;
import lombok.Data;

@Data
final class ClientTimer implements Timer {
  static final int CLIENT_RETRY_MILLIS = 10;

  // Your code here...
  private final Request request;
}
