package dslabs.paxos;

import static dslabs.paxos.ClientTimer.CLIENT_RETRY_MILLIS;

import com.google.common.base.Objects;
import dslabs.atmostonce.AMOCommand;
import dslabs.framework.Address;
import dslabs.framework.Client;
import dslabs.framework.Command;
import dslabs.framework.Node;
import dslabs.framework.Result;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public final class PaxosClient extends Node implements Client {
  private final Address[] servers;

  // Your code here...
  int currentSequence = 0;
  PaxosRequest currentRequest = null;
  Result currentResult = null;
  Address clientAddress = null;

  /* -----------------------------------------------------------------------------------------------
   *  Construction and Initialization
   * ---------------------------------------------------------------------------------------------*/
  public PaxosClient(Address address, Address[] servers) {
    super(address);
    this.servers = servers;
    this.clientAddress = address;
  }

  @Override
  public synchronized void init() {
    // No need to initialize
  }

  /* -----------------------------------------------------------------------------------------------
   *  Client Methods
   * ---------------------------------------------------------------------------------------------*/
  @Override
  public synchronized void sendCommand(Command operation) {
    // Your code here...
    System.out.println("we are here " + this.servers[0] + " " + this.servers[1] + " " + this.servers[2] + " @@ " + this.clientAddress + " --> " + operation);
    //    return;
    currentSequence++;

    AMOCommand x = new AMOCommand(operation, currentSequence, this.clientAddress);
    currentRequest = new PaxosRequest(this.clientAddress, this.currentSequence, x);

    currentResult = null;

    for (int i = 0; i < servers.length; i++) {
      send(this.currentRequest, servers[i]);
    }
    set(new ClientTimer(), CLIENT_RETRY_MILLIS);
  }

  @Override
  public synchronized boolean hasResult() {
    // Your code here...
    return (currentResult != null);
    //    return false;
  }

  @Override
  public synchronized Result getResult() throws InterruptedException {
    // Your code here...
    while (this.currentResult == null) this.wait();
    return this.currentResult;
    //    return null;
  }

  /* -----------------------------------------------------------------------------------------------
   * Message Handlers
   * ---------------------------------------------------------------------------------------------*/
  private synchronized void handlePaxosReply(PaxosReply m, Address sender) {
    // Your code here...
    System.out.println("got REsult " + m);
    if (Objects.equal(currentRequest.sequenceNum(), m.sequenceNum())) {
      System.out.println("we notify over here!!");
      currentResult = m.result().result();
      notify();
    }
  }

  /* -----------------------------------------------------------------------------------------------
   *  Timer Handlers
   * ---------------------------------------------------------------------------------------------*/
  private synchronized void onClientTimer(ClientTimer t) {
    // Your code here...
    if (currentResult == null) {
      // If still no result, resend the current request
      for (Address server : servers) {
        send(currentRequest, server);
      }
      // Reset the timer for another retry
      set(new ClientTimer(), CLIENT_RETRY_MILLIS);
    }
  }
}
