package dslabs.primarybackup;

import static dslabs.primarybackup.ClientTimer.CLIENT_RETRY_MILLIS;

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
class PBClient extends Node implements Client {
  private final Address viewServer;

  // Your code here...
  private Address primaryServer;
  private final Address clientAddress;
  private int currentSequence;
  private Result currentResult;
  private Request currentRequest;
  private GetView viewRequest;
  private ClientTimer clientTimer;
  private View currentView;
  private boolean getViewInprogress;
  private Command currentCommand;

  /* -----------------------------------------------------------------------------------------------
   *  Construction and Initialization
   * ---------------------------------------------------------------------------------------------*/
  public PBClient(Address address, Address viewServer) {
    super(address);
    this.viewServer = viewServer;
    this.clientAddress = address;
    this.primaryServer = null;
    this.currentView = null;
    this.currentSequence = 0;
    this.currentRequest = null;
    this.currentResult = null;
    //    this.clientTimer = null;

  }

  @Override
  public synchronized void init() {
    // Your code here...
    this.viewRequest = new GetView();
    send(this.viewRequest, this.viewServer);
    //    this.clientTimer = new ClientTimer(this.currentRequest);
    //    set(this.clientTimer, CLIENT_RETRY_MILLIS);
  }

  /* -----------------------------------------------------------------------------------------------
   *  Client Methods
   * ---------------------------------------------------------------------------------------------*/
  @Override
  public synchronized void sendCommand(Command command) {
    //     Your code here...
    //    ////System.out.println("i m sending this command "  + command + " " + this.currentView);

    currentSequence++;
    //    System.out.println(command);
    currentRequest =
        new Request(
            new AMOCommand(command, this.currentSequence, this.clientAddress), this.currentView);
    currentResult = null;
    //    this.clientTimer = new ClientTimer(currentRequest);
    if (Objects.equal(this.currentView, null)) {
      send(this.viewRequest, this.viewServer);
    } else {
      currentRequest = new Request(currentRequest.command(), this.currentView);
      send(this.currentRequest, this.primaryServer);
      //      this.clientTimer = new ClientTimer(currentRequest);
      //      set(this.clientTimer, CLIENT_RETRY_MILLIS);
      //// System.out.println("I've sent the command !! " + this.primaryServer + " + " +
      // this.currentView);
    }
    set(new ClientTimer(this.currentRequest), CLIENT_RETRY_MILLIS);
  }

  @Override
  public synchronized boolean hasResult() {
    // Your code here...
    return this.currentResult != null;
  }

  @Override
  public synchronized Result getResult() throws InterruptedException {
    // Your code here...
    while (!hasResult()) this.wait();
    return this.currentResult;
  }

  /* -----------------------------------------------------------------------------------------------
   *  Message Handlers
   * ---------------------------------------------------------------------------------------------*/
  private synchronized void handleReply(Reply m, Address sender) {
    // Your code here...
    if (m.isError()) {
      send(new GetView(), this.viewServer);
      //      send(this.currentRequest, this.primaryServer);
      //      this.clientTimer = new ClientTimer(currentRequest);
      //      set(this.clientTimer,CLIENT_RETRY_MILLIS);
    } else if (!Objects.equal(this.currentRequest, null)
        && Objects.equal(currentRequest.command().sequenceNum(), m.result().sequenceNum())) {
      currentResult = m.result().result();
      //        System.out.println("Got the result " + m);
      //        this.clientTimer.
      //        this.currentRequest = null;
      notify();
    }
  }

  private synchronized void handleViewReply(ViewReply m, Address sender) {
    // Your code here...
    this.primaryServer = m.view().primary();
    this.currentView = m.view();
    if (!this.hasResult()
        && !Objects.equal(this.currentView, null)
        && !Objects.equal(this.currentRequest, null)) {
      currentRequest = new Request(currentRequest.command(), this.currentView);
      //      this.clientTimer = new ClientTimer(currentRequest);
      send(this.currentRequest, this.primaryServer);
      //      set(new ClientTimer(this.currentRequest),CLIENT_RETRY_MILLIS);
    }
  }

  // Your code here...
  /* -----------------------------------------------------------------------------------------------
   *  Timer Handlers
   * ---------------------------------------------------------------------------------------------*/
  private synchronized void onClientTimer(ClientTimer t) {
    if (this.currentSequence == t.request().command().sequenceNum() && currentResult == null) {
      send(new GetView(), this.viewServer);
      if (this.currentView != null) {
        send(new Request(t.request().command(), this.currentView), this.primaryServer);
      }
      set(t, CLIENT_RETRY_MILLIS);
    }
  }
}
