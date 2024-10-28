package dslabs.clientserver;

import static dslabs.clientserver.ClientTimer.CLIENT_RETRY_MILLIS;

import com.google.common.base.Objects;
import dslabs.atmostonce.AMOCommand;
import dslabs.framework.Address;
import dslabs.framework.Client;
import dslabs.framework.Command;
import dslabs.framework.Node;
import dslabs.framework.Result;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Simple client that sends requests to a single server and returns responses.
 *
 * <p>See the documentation of {@link Client} and {@link Node} for important implementation notes.
 */
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
class SimpleClient extends Node implements Client {
  private final Address serverAddress;

  // Your code here...
  private final Address clientAddress;
  private Result currentResult;
  private int currentSequence;
  private Request currentRequest;

  /* -----------------------------------------------------------------------------------------------
   *  Construction and Initialization
   * ---------------------------------------------------------------------------------------------*/
  public SimpleClient(Address address, Address serverAddress) {
    super(address);
    this.clientAddress = address;
    this.serverAddress = serverAddress;
    this.currentResult = null;
    this.currentSequence = 0;
  }

  @Override
  public synchronized void init() {
    // No initialization necessary
  }

  /* -----------------------------------------------------------------------------------------------
   *  Client Methods
   * ---------------------------------------------------------------------------------------------*/
  @Override
  public synchronized void sendCommand(Command command) {
    // Your code here...
    currentSequence++;
    currentRequest = new Request(new AMOCommand(command, this.currentSequence, this.clientAddress));

    currentResult = null;

    send(this.currentRequest, this.serverAddress);
    set(new ClientTimer(currentRequest), CLIENT_RETRY_MILLIS);
  }

  @Override
  public synchronized boolean hasResult() {
    // Your code here...
    return currentResult != null;
  }

  @Override
  public synchronized Result getResult() throws InterruptedException {
    // Your code here...
    while (this.currentResult == null) this.wait();
    return this.currentResult;
  }

  /* -----------------------------------------------------------------------------------------------
   *  Me908ssage Handlers
   * ---------------------------------------------------------------------------------------------*/
  private synchronized void handleReply(Reply m, Address sender) {
    // Your code here...
    if (Objects.equal(currentRequest.command().sequenceNum(), m.result().sequenceNum())) {
      currentResult = m.result().result();
      notify();
    }
  }

  /* -----------------------------------------------------------------------------------------------
   *  Timer Handlers
   * ---------------------------------------------------------------------------------------------*/
  private synchronized void onClientTimer(ClientTimer t) {
    // Your code here...
    if (Objects.equal(currentRequest, t.request()) && currentResult == null) {
      send(currentRequest, serverAddress);
      set(t, CLIENT_RETRY_MILLIS);
    }
  }
}
