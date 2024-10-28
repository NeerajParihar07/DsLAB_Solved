package dslabs.paxos;


import static dslabs.paxos.PingCheckTimer.PING_CHECK_MILLIS;
import static dslabs.paxos.PingTimer.PING_MILLIS;

import dslabs.atmostonce.AMOApplication;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Command;
import dslabs.framework.Node;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class PaxosServer extends Node {
  /** All servers in the Paxos group, including this one. */
  private final Address[] Servers;

  private final AMOApplication<Application> amoApplication;

  // Your code here...
  int slotNumToExecute = 1;
  int cur_SlotNum = 1;
  boolean active;
  HashMap<Integer, PaxosRequest> Proposals;
  HashMap<Integer, PaxosRequest> SlotToCommandDecision;
  HashMap<PaxosRequest, Integer> CommandToSlotDecision;
  HashMap<Integer, PaxosRequest> acceptedValues;
  PaxosRequest curRequest;
  HashSet<pValues> Accepted;
  Ballot curBallot;
  HashSet<Address> GotAdoptedBy;
  HashSet<Address> GotAdoptedByCommander;
  HashSet<Address> mostRecentlyPinged;
  HashSet<Address> ActiveServers;
  Address myAddress = null;
  Address clientAddress = null;
  Address[] acceptorNodes;
  Address[] replicaNodes;

  /* -----------------------------------------------------------------------------------------------
   *  Construction and Initialization
   * ---------------------------------------------------------------------------------------------*/
  public PaxosServer(Address address, Address[] servers, Application app) {
    super(address);
    this.Servers = servers;

    // Your code here...

    this.amoApplication = new AMOApplication<>(app);

    this.myAddress = address;
    this.acceptorNodes = servers;
    this.replicaNodes = servers;

    active = false;
    curRequest = null;

    Proposals = new HashMap<>();
    SlotToCommandDecision = new HashMap<>();
    CommandToSlotDecision = new HashMap<>();
    acceptedValues = new HashMap<>();

    Accepted = new HashSet<>();

    GotAdoptedBy = new HashSet<Address>();
    GotAdoptedByCommander = new HashSet<Address>();
    mostRecentlyPinged = new HashSet<>();
    ActiveServers = new HashSet<>();
  }

  @Override
  public void init() {
    // Your code here...

    System.out.println("Initiated " +  myAddress);

    this.curBallot = new Ballot(0, this.Servers[0]);
    this.active = false;
    Proposals = new HashMap<>();
    ActiveServers.add(this.myAddress);

    // if i am the leader then spawn scouts for all acceptors;
    send(new Ping(1),Servers[0]);
    set(new PingTimer(), PING_MILLIS);

    if (this.myAddress == this.Servers[0]) {
      set(new PingCheckTimer(), PING_CHECK_MILLIS);
      callScout(this.myAddress, acceptorNodes);
    }
  }

  /* -----------------------------------------------------------------------------------------------
   *  Interface Methods
   *
   *  Be sure to implement the following methods correctly. The test code uses them to check
   *  correctness more efficiently.
   * ---------------------------------------------------------------------------------------------*/

  /**
   * Return the status of a given slot in the server's local log.
   *
   * <p>If this server has garbage-collected this slot, it should return {@link
   * PaxosLogSlotStatus#CLEARED} even if it has previously accepted or chosen command for this slot.
   * If this server has both accepted and chosen a command for this slot, it should return {@link
   * PaxosLogSlotStatus#CHOSEN}.
   *
   * <p>Log slots are numbered starting with 1.
   *
   * @param logSlotNum the index of the log slot
   * @return the slot's status
   * @see PaxosLogSlotStatus
   */
  public PaxosLogSlotStatus status(int logSlotNum) {
    // Your code here...
    if (SlotToCommandDecision.containsKey(logSlotNum)) return PaxosLogSlotStatus.CHOSEN;
    else if (acceptedValues.containsKey(logSlotNum)) {
      return PaxosLogSlotStatus.ACCEPTED;
    } else {
      return PaxosLogSlotStatus.EMPTY;
    }

    //    return null;
  }

  /**
   * Return the command associated with a given slot in the server's local log.
   *
   * <p>If the slot has status {@link PaxosLogSlotStatus#CLEARED} or {@link
   * PaxosLogSlotStatus#EMPTY}, this method should return {@code null}. Otherwise, return the
   * command this server has chosen or accepted, according to {@link PaxosServer#status}.
   *
   * <p>If clients wrapped commands in {@link dslabs.atmostonce.AMOCommand}, this method should
   * unwrap them before returning.
   *
   * <p>Log slots are numbered starting with 1.
   *
   * @param logSlotNum the index of the log slot
   * @return the slot's contents or {@code null}
   * @see PaxosLogSlotStatus
   */
  public Command command(int logSlotNum) {
    // Your code here...
    if (SlotToCommandDecision.containsKey(logSlotNum))
      return SlotToCommandDecision.get(logSlotNum).operation.command();
    else if (acceptedValues.containsKey(logSlotNum))
      return acceptedValues.get(logSlotNum).operation.command();
    return null;
  }

  /**
   * Return the index of the first non-cleared slot in the server's local log. The first non-cleared
   * slot is the first slot which has not yet been garbage-collected. By default, the first
   * non-cleared slot is 1.
   *
   * <p>Log slots are numbered starting with 1.
   *
   * @return the index in the log
   * @see PaxosLogSlotStatus
   */
  public int firstNonCleared() {
    // Your code here...
    return 1;
  }

  /**
   * Return the index of the last non-empty slot in the server's local log, according to the defined
   * states in {@link PaxosLogSlotStatus}. If there are no non-empty slots in the log, this method
   * should return 0.
   *
   * <p>Log slots are numbered starting with 1.
   *
   * @return the index in the log
   * @see PaxosLogSlotStatus
   */
  public int lastNonEmpty() {
    // Your code here...
    if (SlotToCommandDecision.isEmpty() && acceptedValues.isEmpty()) {
      return 0;
    } else {
      int maxDecisionSlot =
          SlotToCommandDecision.keySet().stream().mapToInt(Integer::intValue).max().orElse(0);
      int maxAcceptedSlot =
          acceptedValues.keySet().stream().mapToInt(Integer::intValue).max().orElse(0);
      return Math.max(maxDecisionSlot, maxAcceptedSlot);
    }
    //    return 0;
  }

  /* -----------------------------------------------------------------------------------------------
   *  Message Handlers
   * ---------------------------------------------------------------------------------------------*/
  private void handlePaxosRequest(PaxosRequest m, Address sender) {
    // Your code here...
    this.curRequest = m;

    this.clientAddress = sender;

    // let's assume there is only 1 leader with leader id or the first server;
//    System.out.println("we got your request " + m + " at " + this.myAddress + " sending it to the leader at " + Servers[0]);
    propose(m);
  }

  void propose(PaxosRequest m) {
    //////System.println("Yaha toh ponch rhe hai!! "  + m + CommandToSlotDecision.containsKey(m));
    if (!CommandToSlotDecision.containsKey(m)) {
//      Proposals.put(cur_SlotNum, m);
      for (int i = 1; ; i++) {
        //System.println("checking the slot " +  i + " decision is " + (!Proposals.containsKey(i) && !SlotToCommandDecision.containsKey(i)));
        if (!Proposals.containsKey(i) && !SlotToCommandDecision.containsKey(i)) {
          Proposals.put(i, m);
//          //System.println("we are send Propose Message for request " + m + " at " + this.myAddress + " and slot "  + i);
          send(new ProposeMessage(m.clientAddress, i, m), this.Servers[0]);
          break;
        }
      }
    }
  }

  void handleProposeMessage(ProposeMessage m, Address sender) {
//    System.out.println("we got Propose Message for request " + m + " at " + this.myAddress + " active " + active );
    if (!Proposals.containsKey(m.slot_num) || (Proposals.containsKey(m.slot_num) && Objects.equals(Proposals.get(m.slot_num), m.operation))) {
      //System.println(" SNum "  + m.slot_num + " operation " );
//      if (!Objects.equals(Proposals.get(SNum), m.operation)) {
        if (active) {
          //////System.println("Calling commander from " + this.myAddress);
          callCommander(
              this.myAddress,
              acceptorNodes,
              replicaNodes,
              new pValues(curBallot, m.slot_num, m.operation));
        }
//      }
    }
  }

  void perform(PaxosRequest m, Address sender) {
    //System.println("welcome to perform " + m);
    for (int i = 1; i < slotNumToExecute; i++) {
      if (SlotToCommandDecision.containsKey(i) && Objects.equals(SlotToCommandDecision.get(i), m)) {
        slotNumToExecute++;
        return;
      }
    }
    AMOResult result = this.amoApplication.execute(m.operation);
    //System.println("Done processing " + m);
    // yaha merko command execute krni hai and send the response back to client
    slotNumToExecute++;
    send(new PaxosReply(m.sequenceNum, result), this.clientAddress);
  }

  // Your code here...
  void callScout(Address sender, Address[] acceptors) {
    // think of waitfor, for the time being let's create a hashmap
    System.out.println("we are coming here !! ");
    GotAdoptedBy.clear();
    for (int i = 0; i < acceptors.length; i++) {
      Address address = acceptors[i];
      if(!ActiveServers.contains(address))continue;
      System.out.println("sending P1aMessage to " + address);
      // Perform operations with 'address'
      P1aMessage a = new P1aMessage(this.myAddress, curBallot);
      GotAdoptedBy.add(address);
      send(a, address);
    }
  }

  void handleDecisionMessage(DecisionMessage m, Address sender) {
    //System.println("welcome to decision message " + m + " SlotNum to execute " + slotNumToExecute);
    SlotToCommandDecision.put(m.slot_num, m.operation);
    while (SlotToCommandDecision.containsKey(slotNumToExecute)) {
      if (Proposals.containsKey(slotNumToExecute)
          && !Objects.equals(
              Proposals.get(slotNumToExecute), SlotToCommandDecision.get(slotNumToExecute))) {
        propose(Proposals.get(slotNumToExecute));
      }
      perform(SlotToCommandDecision.get(slotNumToExecute), sender);
//      slotNumToExecute++;
    }
  }

  void handleP1aMessage(P1aMessage m, Address sender) {
//    System.out.println("In P1a Message by scout at " + sender + " " + myAddress + " " + curBallot + " ==  " + m.ballot);
    if (compare(m.ballot, this.curBallot) < 0) this.curBallot = m.ballot;
    send(new P1bMessage(this.Servers[0], curBallot, Accepted), this.Servers[0]);
  }

  void callCommander(Address sender, Address[] acceptorNodes, Address[] replicaNodes, pValues cur) {
//    System.out.println("commander has been called at " + sender + " with cur value " + cur);
    GotAdoptedByCommander.clear();
    for (int i = 0; i < acceptorNodes.length; i++) {
      if(!ActiveServers.contains(acceptorNodes[i]))continue;
      //////System.println("Sending P2aMessage from " + this.myAddress +  " to " +acceptorNodes[i]);
      send(new P2aMessage(sender, cur), acceptorNodes[i]);
      GotAdoptedByCommander.add(acceptorNodes[i]);
    }
  }

  void handleP2aMessage(P2aMessage m, Address sender) {
    //System.println("About to send a P2a Message " + m + " with this ballot " + curBallot);
    if (compare(m.pValue.ballot, this.curBallot) <= 0) {
      //      GotAdoptedByCommander.remove(sender);
      this.curBallot = m.pValue.ballot;
      //      Accepted.put(m.ballot);
      acceptedValues.put(m.pValue.slot_num, m.pValue.operation);
    }
    send(
        new P2bMessage(
            this.Servers[0], new pValues(this.curBallot, m.pValue.slot_num, m.pValue.operation)),
        this.Servers[0]);
  }

  void handleP1bMessage(P1bMessage m, Address sender) {
//    System.println("In P1bMessage at  " + this.myAddress + " " + sender + " " + m.ballot + " == " + curBallot);
    if (Objects.equals(m.ballot, this.curBallot)) {
      //System.println("ISKE ANDar");
      GotAdoptedBy.remove(sender);
      Accepted.addAll(m.accepted);
      //System.println("size " + GotAdoptedBy.size() + " length " +  Servers.length);

      int sz = ActiveServers.isEmpty()?0:ActiveServers.size();
      System.out.println("SSZZ " + sz + " " + Servers.length + " " + GotAdoptedBy.size());
      if (GotAdoptedBy.size() < (sz + 1)/ 2)
        send(new AdoptedMessage(this.myAddress, curBallot, Accepted), this.Servers[0]);
    } else {
      send(new PreemptedMessage(this.myAddress, m.ballot), this.Servers[0]);
    }
  }

  void handleP2bMessage(P2bMessage m, Address sender) {
    if (Objects.equals(m.pValue.ballot, this.curBallot)) {
      GotAdoptedByCommander.remove(sender);
      //      Accepted.addAll(m.);
      int sz = ActiveServers.isEmpty()?0:ActiveServers.size();
      if (GotAdoptedByCommander.size() < (sz + 1)/ 2) {
        for (int i = 0; i < replicaNodes.length; i++) {
          send(
              new DecisionMessage(replicaNodes[i], m.pValue.slot_num, m.pValue.operation),
              replicaNodes[i]);
        }
      }
    } else {
      send(new PreemptedMessage(this.myAddress, curBallot), this.Servers[0]);
    }
  }

  private void handleAdoptedMessage(AdoptedMessage msg, Address sender) {
    //System.println("Adopted Message has been called at " + this.myAddress + " " + msg);
    if (curBallot.equals(msg.ballot)) {
      Map<Integer, pValues> pmax = new HashMap<>();
      for (pValues pv : msg.accepted) {
        int sn = pv.slot_num;
        Ballot bn = pv.ballot;
        if (!pmax.containsKey(sn) || compare(pmax.get(sn).ballot, bn) < 0) {
          pmax.put(sn, pv);
          Proposals.put(sn, pv.operation);
        }
      }
      for (Map.Entry<Integer, PaxosRequest> entry : Proposals.entrySet()) {
        int sn = entry.getKey();
        PaxosRequest cmd = entry.getValue();
        // call commander_________________________________
        callCommander(
            this.myAddress, acceptorNodes, replicaNodes, new pValues(msg.ballot, sn, cmd));
      }
      active = true;
    }
  }

  private void handlePreemptedMessage(PreemptedMessage msg, Address sender) {
    if (compare(msg.ballot, this.curBallot) > 0) {
      active = false;
      curBallot = new Ballot(msg.ballot.ballot_num + 1, this.myAddress);
      // Start a new scout with the updated ballot number____________________________
      callScout(myAddress, acceptorNodes);
    }
  }

  int compare(Ballot x, Ballot y) {
    if (x.ballot_num() != y.ballot_num()) return (x.ballot_num > y.ballot_num ? 1 : -1);
    return x.leader.compareTo(y.leader);
  }

  /* -----------------------------------------------------------------------------------------------
   *  Timer Handlers
   * ---------------------------------------------------------------------------------------------*/
  // Your code here...

  private void handlePing(Ping m, Address sender) {
    System.out.println("Got the ping from " + sender);
    mostRecentlyPinged.add(sender);
  }

  private void onPingCheckTimer(PingCheckTimer t) {
    System.out.println("Yo " + mostRecentlyPinged.size());
    int ok = 0;
    for (Address address : mostRecentlyPinged) {
      System.out.println(address);
      ActiveServers.add(address);
    }

    mostRecentlyPinged.clear();
    set(new PingCheckTimer(), PING_CHECK_MILLIS);
  }

  private void onPingTimer(PingTimer t) {
    System.out.println("I;m " + myAddress + " sending ping "+  this.Servers[0]);
    send(new Ping(1), this.Servers[0]);
    set(new PingTimer(), PING_MILLIS);
  }

  /* -----------------------------------------------------------------------------------------------
   *  Utils
   * ---------------------------------------------------------------------------------------------*/
  // Your code here...

}
