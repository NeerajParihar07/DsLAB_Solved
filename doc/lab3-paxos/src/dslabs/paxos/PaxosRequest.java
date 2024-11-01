package dslabs.paxos;

import dslabs.atmostonce.AMOCommand;
import dslabs.framework.Address;
import dslabs.framework.Message;
import lombok.Data;

@Data
public final class PaxosRequest implements Message {
  //
  final Address clientAddress;
  final int sequenceNum;
  final AMOCommand operation;
}
