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

    //////System.out.println("Initiated " +  myAddress);

    this.curBallot = new Ballot(0, this.Servers[0]);
    this.active = false;
    Proposals = new HashMap<>();
    ActiveServers.add(this.myAddress);

    // if i am the leader then spawn scouts for all acceptors;
    if(NotMe(Servers[0]))send(new Ping(slotNumToExecute, SlotToCommandDecision),Servers[0]);
    set(new PingTimer(), PING_MILLIS);
    set(new PingCheckTimer(), PING_CHECK_MILLIS);

//    if (this.myAddress == this.Servers[0]) {
      callScout(this.myAddress, acceptorNodes);
//    }
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
    //System.out.println("PaxosRequest ------>    " + m  + " from " + sender + " at " + myAddress);
    propose(m);
  }

  void propose(PaxosRequest m) {
  if (!CommandToSlotDecision.containsKey(m)) {
      for (int i = slotNumToExecute; i <= slotNumToExecute + 5; i++) {
       if (!SlotToCommandDecision.containsKey(i)) {
          Proposals.put(i, m);
          //System.out.println("ProposeFunction ------>    " + m  + " for slot " + i + " at "+  myAddress);
          if(NotMe(Servers[0]))send(new ProposeMessage(m.clientAddress, i, m), this.Servers[0]);
          break;
        }
      }
    }
  }

  void handleProposeMessage(ProposeMessage m, Address sender) {
    callScout(this.myAddress, acceptorNodes);
    if (!Proposals.containsKey(m.slot_num) || (Proposals.containsKey(m.slot_num) && Objects.equals(Proposals.get(m.slot_num), m.operation))) {
        if (active) {
          //System.out.println("ProposeMessage ------>    " + m  + " from " + sender + " at " + myAddress);
          callCommander(
              this.myAddress,
              acceptorNodes,
              replicaNodes,
              new pValues(curBallot, m.slot_num, m.operation));
        }
    }
  }

  void callCommander(Address sender, Address[] acceptorNodes, Address[] replicaNodes, pValues cur) {
    GotAdoptedByCommander.clear();
    for (int i = 0; i < acceptorNodes.length; i++) {
      Address address = acceptorNodes[i];
      GotAdoptedByCommander.add(address);
//      System.out.println("CallCommanderFunction ------>    " + cur  + " from " + sender + " at " + myAddress);
      send(new P2aMessage(this.myAddress, cur), acceptorNodes[i]);
    }
  }

  void handleP2aMessage(P2aMessage m, Address sender) {
    if (compare(m.pValue.ballot, this.curBallot) >= 0) {
      this.curBallot = m.pValue.ballot;
      acceptedValues.put(m.pValue.slot_num, m.pValue.operation);
    }
    //System.out.println("P2aMessage ------>    " + m  + " from " + sender + " at " + myAddress);
    if(NotMe(m.leaderAddress))send(
        new P2bMessage(
            this.myAddress, new pValues(this.curBallot, m.pValue.slot_num, m.pValue.operation)),
        m.leaderAddress);
  }

  void perform(PaxosRequest m, Address sender) {
    for (int i = 1; i < slotNumToExecute; i++) {
      if (SlotToCommandDecision.containsKey(i) && Objects.equals(SlotToCommandDecision.get(i), m)) {
        slotNumToExecute++;
        return;
      }
    }
    AMOResult result = this.amoApplication.execute(m.operation);
    ////System.out.println("PerformFunction ------>    " + m  + " from " + sender + " at " + myAddress);
    slotNumToExecute++;
    if(NotMe(clientAddress))send(new PaxosReply(m.sequenceNum, result), m.clientAddress);
  }

  // Your code here...
  void callScout(Address sender, Address[] acceptors) {
    GotAdoptedBy.clear();
    for (int i = 0; i < acceptors.length; i++) {
      Address address = acceptors[i];
      GotAdoptedBy.add(acceptors[i]);
//      if(!ActiveServers.contains(address))continue;
      //System.out.println("CallScoutFunction ------>    " + curBallot  + " from " + sender + " at " + myAddress);
      if(NotMe(address))send(new P1aMessage(this.myAddress, curBallot), address);
    }
  }

  void handleDecisionMessage(DecisionMessage m, Address sender) {
    SlotToCommandDecision.put(m.slot_num, m.operation);
    while (SlotToCommandDecision.containsKey(slotNumToExecute)) {
      if (Proposals.containsKey(slotNumToExecute)
          && !Objects.equals(
              Proposals.get(slotNumToExecute), SlotToCommandDecision.get(slotNumToExecute))) {
        //System.out.println("DecisionMessage ------>    " + m  + " from " + sender + " at " + myAddress);
        //System.out.println("DecisionMessage sending this proposal again ------>    " + Proposals.get(slotNumToExecute)  + " from " + sender + " at " + myAddress);
        propose(Proposals.get(slotNumToExecute));
      }
//      System.out.println("SlotNumToExecute -- :> " + slotNumToExecute);
      //System.out.println("DecisionMessage performing------>    " + SlotToCommandDecision.get(slotNumToExecute)  + " from " + sender + " at " + myAddress);
      perform(SlotToCommandDecision.get(slotNumToExecute),sender);
    }
  }

  void handleP1aMessage(P1aMessage m, Address sender) {
//    System.out.println("P1aMessage ------>    " + curBallot  + " from " + sender + " at " + myAddress);
    if (compare(m.ballot, this.curBallot) > 0) this.curBallot = m.ballot;
    for(Address address: Servers)send(new P1bMessage(this.myAddress, curBallot, Accepted), address);
  }

  void handleP1bMessage(P1bMessage m, Address sender) {
//    System.out.println("Neeraj Parihar " + m + " " +  sender);
    if (!Objects.equals(m.ballot, curBallot)) {
      //System.out.println("YES WE ARE SENDING PREMPT FOR " + m.ballot + " " + curBallot  + sender);
      send(new PreemptedMessage(myAddress, m.ballot), Servers[0]);
      return;
    }

    GotAdoptedBy.remove(sender);
    Accepted.addAll(m.accepted);

    int majority = Servers.length / 2;
//    System.out.println( " YOUUO " + sender + " ** " + GotAdoptedBy + " -- > " + (Servers.length - GotAdoptedBy.size()) +  " Majority " + majority);
    if ((Servers.length - GotAdoptedBy.size()) > majority) {
      //System.out.println("P1bMessage ------>    " + m  + " from " + sender + " at " + myAddress);
      send(new AdoptedMessage(myAddress, curBallot, Accepted), Servers[0]);
    }
  }

  void handleP2bMessage(P2bMessage m, Address sender) {
    //System.out.println("P2bMessage ------>    " + m  + " from " + sender + " at " + myAddress);

    if (!Objects.equals(m.pValue.ballot, curBallot)) {
      send(new PreemptedMessage(myAddress, curBallot), Servers[0]);
      return;
    }

    GotAdoptedByCommander.remove(sender);

    if ((Servers.length - GotAdoptedBy.size()) > Servers.length / 2) {
      SlotToCommandDecision.putIfAbsent(m.pValue.slot_num, m.pValue.operation);
      CommanderResponseMessage res = new CommanderResponseMessage(myAddress, m.pValue.slot_num, SlotToCommandDecision.get(m.pValue.slot_num));
      //System.out.println("Here is the decision for this " + m + " response " + res);
      send(res, Servers[0]);
    }
  }

  void handleCommanderResponseMessage(CommanderResponseMessage m, Address sender)
  {
    SlotToCommandDecision.putIfAbsent(m.slot_num, m.operation);
    for (Address replica : replicaNodes) {
      if (!NotMe(replica)) continue;
//      //System.out.println("CommanderResponseMessage ------>    " + m  + " from " + sender + " at " + myAddress);
      send(new DecisionMessage(replica, m.slot_num, SlotToCommandDecision.get(m.slot_num)), replica);
    }
  }

  private void handleAdoptedMessage(AdoptedMessage msg, Address sender) {
//    //System.out.println("AdoptedMessage ------>    " + msg  + " from " + sender + " at " + myAddress);
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
      //System.out.println("PreemptedMessage ------>    " + msg  + " from " + sender + " at " + myAddress);
      callScout(myAddress, acceptorNodes);
    }
  }

  int compare(Ballot x, Ballot y) {
    if (x.ballot_num() != y.ballot_num()) return (x.ballot_num > y.ballot_num ? 1 : -1);
    return x.leader.compareTo(y.leader);
  }

  private void handlePing(Ping m, Address sender) {
//    System.out.println("@@@@@@@@  --> " + slotNumToExecute  +  " new " + m +  " old list " +  SlotToCommandDecision);
    if(slotNumToExecute < m.CurrentSlotToExecute)SlotToCommandDecision = m.decision_list;
    mostRecentlyPinged.add(sender);
    send(new AckMessage(myAddress), this.Servers[0]);
  }

  void handleAckMessage(AckMessage m, Address sender)
  {
    // right now let's not do anything.
  }

  private void onPingCheckTimer(PingCheckTimer t) {
    mostRecentlyPinged.add(myAddress); // Include self
    ActiveServers.clear();
    ActiveServers.addAll(mostRecentlyPinged);
    mostRecentlyPinged.clear();
//    int id = 0;
//    for(int i = Servers.length-1 ; i >= 0; i--)if(ActiveServers.contains(Servers[i]))id = i;
//    Address temp = Servers[0];
//    Servers[0] = Servers[id];
//    Servers[id] = temp;

    mostRecentlyPinged.clear();
    set(new PingCheckTimer(), PING_CHECK_MILLIS);
  }

  private void onPingTimer(PingTimer t) {
    if(this.myAddress != Servers[0])return;
    for(Address server: Servers)if(NotMe(this.Servers[0]))send(new Ping(slotNumToExecute, SlotToCommandDecision), server);
    set(new PingTimer(), PING_MILLIS);
  }

//  private void handleAckMessage(AckMessage msg, Address sender)
//  {
//    for(int key: msg.decision_list.keySet())
//    {
//
//    }
//  }
  
  private boolean NotMe(Address x)
  {
    return true;
//    return !Objects.equals(this.myAddress, x);
  }
}
