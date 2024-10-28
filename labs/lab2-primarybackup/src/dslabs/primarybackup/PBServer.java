package dslabs.primarybackup;

import static dslabs.primarybackup.FRTimer.FR_MILIS;
import static dslabs.primarybackup.PingTimer.PING_MILLIS;
import static dslabs.primarybackup.StTransferTimer.STATE_TRANSFER_MILLIS;

import com.google.common.base.Objects;
import dslabs.atmostonce.AMOApplication;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Node;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
class PBServer extends Node {
  private final Address viewServer;
  private AMOApplication<Application> amoApplication;
  private View view;
  private Address myaddress;
  private PingTimer pingTimer;
  private Request currentRequest;
  private int mostRecentState;
  private StateTransfer stRequest;
  private boolean stProgress;
  private ForwardedRequest forwardRequest;
  private boolean requestProgress;
  private boolean stACK;
  private boolean frACK;

  /* -----------------------------------------------------------------------------------------------
   *  Construction and Initialization
   * ---------------------------------------------------------------------------------------------*/
  PBServer(Address address, Address viewServer, Application app) {
    super(address);
    this.viewServer = viewServer;
    this.myaddress = address;
    this.amoApplication = new AMOApplication<>(app);
    this.view = null;
    this.currentRequest = null;
    this.forwardRequest = null;
    this.mostRecentState = 0;
    stRequest = null;
    stProgress = false;
    this.requestProgress = false;
    this.stACK = false;
    this.frACK = false;
  }

  @Override
  public void init() {
    this.pingTimer = new PingTimer(new Ping(0));
    send(new Ping(0), this.viewServer);
    set(this.pingTimer, PING_MILLIS);
  }

  private void handleRequest(Request m, Address sender) {
    if (this.view == null) return;
    //    if(!Objects.equal(this.view, m.currentView()))
    //    {
    //      ////System.out.println("Got Different view " + this.view + " " + m.currentView() + " " +
    // this.stProgress );
    //      send(new Reply(null, true), sender);
    //      return;
    //    }
    ////// System.out.println("Same view " + this.view + " " + m.currentView() );
    //// System.out.println("These are the flags " + this.stProgress + " " + this.requestProgress +
    // " " + this.currentRequest);

    boolean res = (requestProgress && !Objects.equal(m, this.currentRequest));
    //    System.out.println("coming in !! " + stProgress + " RP " + res);
    if (this.stProgress || res) return;
    //////// System.out.println("State Before the request " + m + " " + this.view);
    //    System.out.println("Request " + m + " sender " + sender);
    this.requestProgress = true;
    this.currentRequest = m;
    if (Objects.equal(this.view.backup(), null)
        || Objects.equal(this.view.primary(), this.view.backup())) {
      executeRequest(this.currentRequest, sender);
      this.requestProgress = false;
      //      //////System.out.println("Processed U the request at primary " + m + " " +
      // this.myaddress + " " + this.view + " " + sender);
    } else forwardRequest(this.currentRequest);
  }

  private void handleForwardedRequest(ForwardedRequest m, Address sender) {
    ////// System.out.println("Got the request at backup" + this.myaddress + " " + m + " " + sender
    // + " " + this.view);
    if (Objects.equal(this.view, m.view())) {
      this.forwardRequest = m;
      executeRequest(m.request(), m.request().command().address());
      send(new AckMessageFR(true, m.request().command().sequenceNum()), m.address());
      //        set(new ACKFTimer(new AckMessageFR(true, m.request().command().sequenceNum()),
      // m.address()), ACKF_MILIS);
    }
  }

  private void handleViewReply(ViewReply m, Address sender) {
    if (this.stProgress) return;
    else if (this.view == null) this.view = m.view();
    else if ((this.view.viewNum() != m.view().viewNum()
        && Objects.equal(this.view.primary(), m.view().primary())
        && Objects.equal(this.view.backup(), m.view().backup())))
      return; // view changes because of idle server
    else if (this.view.viewNum() > m.view().viewNum()) return;
    else if (isStateTransfer(m)) {
      ////// System.out.println("Current View " + this.view + " New View " + m.view());
      //      this.stProgress = true;
      if (m.view().backup() == null) {
        this.view = m.view();
        //        this.stProgress = false;
      } else performST(m);
    }
    //    set(new PingTimer(new Ping(this.view.viewNum())), PING_MILLIS);
  }

  private void onPingTimer(PingTimer t) {
    //    if(this.view == null || t.request().viewNum() == this.view.viewNum()){
    //      send(t.request(), this.viewServer);
    //      set(t, PING_MILLIS);
    //    }
    send(new Ping(this.view != null ? this.view.viewNum() : 0), this.viewServer);
    set(new PingTimer(new Ping(this.view != null ? this.view.viewNum() : 0)), PING_MILLIS);
  }

  private void handleStateTransfer(StateTransfer m, Address sender) {
    ////// System.out.println("MESSI " + m);
    if (mostRecentState >= m.view().viewNum() || this.stProgress == true) {
      if (mostRecentState == m.view().viewNum())
        send(new AckMessageST(m.view(), true), m.address());
      return;
    }
    this.stRequest = m;
    this.amoApplication = new AMOApplication<>(m.amoApplication().application());
    mostRecentState = m.view().viewNum();
    this.view = new View(m.view().viewNum(), m.view().primary(), m.view().backup());
    ////// System.out.println("RONALDO " + m);
    send(new AckMessageST(m.view(), true), m.address());
    //      set(new ACKTimer(new AckMessageST(m.view(),true), m.address()), ACK_MILIS);
  }

  private void handleAckMessageST(AckMessageST m, Address sender) {
    ////// System.out.println("GOAT " + this.view + "  " + m.view());
    if (this.view.viewNum() >= m.view().viewNum()) return;
    ////// System.out.println("GOT THIS RE "+ m);
    this.stACK = m.ack();
    this.view = new View(m.view().viewNum(), m.view().primary(), m.view().backup());
    this.stRequest = null;
    this.stProgress = false;
    //    set(new PingTimer(new Ping(this.view.viewNum())), PING_MILLIS);
  }

  private void handleAckMessageFR(AckMessageFR m, Address sender) {
    //    //////System.out.println("Got the request at primary " + m + " " + this.myaddress + " " +
    // this.view + " " + sender);
    if (this.stProgress || m.seqNum() != this.currentRequest.command().sequenceNum()) return;
    if (!Objects.equal(this.currentRequest, null)) {
      executeRequest(this.currentRequest, this.currentRequest.command().address());
      this.frACK = m.ack();
      this.forwardRequest = null;
      this.requestProgress = false;
    }
    //    //////System.out.println("Processed L the request at primary " + m + " " + this.myaddress
    // + " " + this.view + " " + sender);
  }

  private void onFRTimer(FRTimer t) {
    if (Objects.equal(this.currentRequest, t.request().request())) {
      ////// System.out.println(" Sending the FR " + t.request());
      send(t.request(), t.destination());
      set(t, FR_MILIS);
    }
  }

  private void onStTransferTimer(StTransferTimer t) {
    if (Objects.equal(this.stRequest, t.request()) && this.stProgress == true) {
      ////// System.out.println("sent the request timer " + t.request());
      send(t.request(), t.destination());
      set(t, STATE_TRANSFER_MILLIS);
    }
  }

  //  private void onACKTimer(ACKTimer t) {
  //    if(Objects.equal(this.view, t.request().view()))
  //    {
  //      //////System.out.println(" GOT sending back the request " + this.view + " " +
  // t.request());
  //      send(t.request(), t.destination());
  //      set(t, ACK_MILIS);
  //    }
  //  }

  //  private void onACKFTimer(ACKFTimer t) {
  //    if(Objects.equal(this.forwardRequest.request().command().sequenceNum(),
  // t.request().seqNum()))
  //    {
  //      send(t.request(), t.destination());
  //      set(t, ACKF_MILIS);
  //    }
  //  }

  private boolean isStateTransfer(ViewReply m) {
    return (!Objects.equal(this.view, m.view())
        && Objects.equal(this.myaddress, m.view().primary())
        && this.view.viewNum() < m.view().viewNum()
        && (Objects.equal(this.myaddress, this.view.primary())
            || Objects.equal(this.myaddress, this.view.backup())));
  }

  private void performST(ViewReply m) {
    ////// System.out.println("sent the request "  + m);
    this.stProgress = true;
    this.requestProgress = false;
    this.stACK = false;
    this.stRequest = new StateTransfer(this.amoApplication, this.myaddress, m.view());
    send(this.stRequest, m.view().backup());
    //    set(new StTransferTimer(this.stRequest, m.view().backup()), STATE_TRANSFER_MILLIS);
  }

  private void executeRequest(Request m, Address sender) {
    if (this.myaddress == null || stProgress) return;
    //////// System.out.println("Executing this command at " + this.myaddress + "  " + m +  " " +
    // this.view);
    AMOResult result = this.amoApplication.execute(m.command());
    if (result != null && this.view.primary() == this.myaddress)
      send(new Reply(result, false), sender);
  }

  private void forwardRequest(Request m) {
    ////// System.out.println("Yes we are here " + this.currentRequest + " " + m + " " +  stProgress
    // +"  "+ (!Objects.equal(this.currentRequest, m)));
    if (stProgress || !Objects.equal(this.currentRequest, m)) return;
    this.frACK = true;
    ////// System.out.println(" Forwarding the request ");
    this.forwardRequest = new ForwardedRequest(m, this.myaddress, this.view);
    send(forwardRequest, view.backup());
    set(new FRTimer(forwardRequest, view.backup()), FR_MILIS);
  }
}
