package dslabs.primarybackup;

import dslabs.framework.Address;
import dslabs.framework.Timer;
import lombok.Data;

@Data
final class PingCheckTimer implements Timer {
  static final int PING_CHECK_MILLIS = 100;
}

@Data
final class PingTimer implements Timer {
  static final int PING_MILLIS = 25;
  private final Ping request;
}

@Data
final class ClientTimer implements Timer {
  static final int CLIENT_RETRY_MILLIS = 100; // 100

  // Your code here...
  private final Request request;
}

// Your code here...
@Data
final class StTransferTimer implements Timer {
  private final StateTransfer request;
  private final Address destination;
  static final int STATE_TRANSFER_MILLIS = 25;
}

@Data
final class FRTimer implements Timer {
  private final ForwardedRequest request;
  private final Address destination;
  static final int FR_MILIS = 50;
}

@Data
final class ACKTimer implements Timer {
  private final AckMessageST request;
  private final Address destination;
  static final int ACK_MILIS = 100;
}

@Data
final class ACKFTimer implements Timer {
  private final AckMessageFR request;
  private final Address destination;
  static final int ACKF_MILIS = 100;
}
