package dslabs.paxos;

import dslabs.framework.Address;
import dslabs.framework.Message;
import java.util.HashSet;
import lombok.Data;

// Your code here...

@Data
final class Ping implements Message {
  private final int health;
}

@Data
final class Ballot implements Message {
  //
  final int ballot_num;
  final Address leader;
}

@Data
final class pValues implements Message {
  //
  final Ballot ballot; // this is not a integer but a pair of integer and leader id
  final int slot_num;
  final PaxosRequest operation;
}

@Data
final class P1aMessage implements Message {
  //
  final Address leaderAddress;
  final Ballot ballot;
}

@Data
final class P1bMessage implements Message {
  //
  final Address leaderAddress;
  final Ballot ballot;
  final HashSet<pValues> accepted; // this should be a hashmap of all the accepted values so far
  // need to implement this
}

@Data
final class P2aMessage implements Message {
  //
  final Address leaderAddress;
  final pValues pValue;
}

@Data
final class P2bMessage implements Message {
  //
  final Address leaderAddress;
  final pValues pValue;
}

@Data
final class PreemptedMessage implements Message {
  final Address address;
  final Ballot ballot;
}

@Data
final class DecisionMessage implements Message {
  //
  final Address replicaAddress;
  final int slot_num;
  final PaxosRequest operation;
}

@Data
final class AdoptedMessage implements Message {
  //
  final Address address;
  final Ballot ballot;
  final HashSet<pValues> accepted;
}

@Data
final class ProposeMessage implements Message {
  //
  final Address clientAddress;
  final int slot_num;
  final PaxosRequest operation;
}
