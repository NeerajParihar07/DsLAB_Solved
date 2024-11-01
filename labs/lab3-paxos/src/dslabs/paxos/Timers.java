package dslabs.paxos;

import dslabs.framework.Timer;
import lombok.Data;

@Data
final class ClientTimer implements Timer {
  static final int CLIENT_RETRY_MILLIS = 100;

  // Your code here...
}

// Your code here...
@Data
final class PingCheckTimer implements Timer {
  static final int PING_CHECK_MILLIS = 75;

  // Your code here...
}

@Data
final class PingTimer implements Timer {
  static final int PING_MILLIS = 25;

  // Your code here...
}
