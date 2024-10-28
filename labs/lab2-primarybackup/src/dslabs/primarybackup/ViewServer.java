package dslabs.primarybackup;

import static dslabs.primarybackup.PingCheckTimer.PING_CHECK_MILLIS;

import dslabs.framework.Address;
import dslabs.framework.Node;
import java.util.HashMap;
import java.util.HashSet;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
class ViewServer extends Node {
  static final int STARTUP_VIEWNUM = 0;
  private static final int INITIAL_VIEWNUM = 1;

  // Your code here...
  private View currentView;
  private View prevView;
  private int viewNum;
  private int lastAckStateNum;
  private Address client;
  private HashSet<Address> mostRecentlyPinged;
  private HashSet<Address> ActiveServers;
  private HashMap<Address, Integer> addressTOPing;

  /* -----------------------------------------------------------------------------------------------
   *  Construction and Initialization
   * ---------------------------------------------------------------------------------------------*/
  public ViewServer(Address address) {
    super(address);
  }

  @Override
  public void init() {
    set(new PingCheckTimer(), PING_CHECK_MILLIS);
    // Your code here...
    currentView = new View(STARTUP_VIEWNUM, null, null);
    lastAckStateNum = STARTUP_VIEWNUM;
    prevView = new View(currentView.viewNum(), currentView.primary(), currentView.backup());
    viewNum = 1;
    mostRecentlyPinged = new HashSet<>();
    ActiveServers = new HashSet<>();
    addressTOPing = new HashMap<Address, Integer>();
    client = null;
  }

  /* -----------------------------------------------------------------------------------------------
   *  Message Handlers
   * ---------------------------------------------------------------------------------------------*/
  private void handlePing(Ping m, Address sender) {
    //    System.out.println(" U got a new ping " + m + " from " + sender + " cv " + currentView);
    if (m.viewNum() != STARTUP_VIEWNUM
        && addressTOPing.containsKey(sender)
        && (addressTOPing.get(sender) - 1 > m.viewNum())) return;

    mostRecentlyPinged.add(sender);

    if (currentView.viewNum() == STARTUP_VIEWNUM) {
      currentView = new View(INITIAL_VIEWNUM, sender, null);
      // System.out.println( "View is updated "+currentView);

    } else if (sender.equals(currentView.primary()) && m.viewNum() == currentView.viewNum()) {
      lastAckStateNum = currentView.viewNum();
      prevView = new View(currentView.viewNum(), currentView.primary(), currentView.backup());
    }

    getNewUpdate();
    addressTOPing.put(sender, currentView.viewNum());
    //    System.out.println(" L got a new ping " + m + " from " + sender + " cv " + currentView);
    send(new ViewReply(currentView), sender);
  }

  private void handleGetView(GetView m, Address sender) {
    // Your code here...
    send(new ViewReply(currentView), sender);
    client = sender;
  }

  /* -----------------------------------------------------------------------------------------------
   *  Timer Handlers
   * ---------------------------------------------------------------------------------------------*/
  private void onPingCheckTimer(PingCheckTimer t) {
    Address invalidServer = null;
    for (Address aliveServer : ActiveServers) {
      if (!mostRecentlyPinged.contains(aliveServer)) {
        invalidServer = aliveServer;
      }
    }
    if (invalidServer != null) {
      ActiveServers.remove(invalidServer);
      getValidState(invalidServer);
    }

    for (Address address : mostRecentlyPinged) {
      ActiveServers.add(address);
    }
    mostRecentlyPinged.clear();

    getNewUpdate();
    set(t, PING_CHECK_MILLIS);
  }

  /* -----------------------------------------------------------------------------------------------
   *  Utils
   * ---------------------------------------------------------------------------------------------*/
  // Your code here...

  private void getValidState(Address server) {
    if (currentView.viewNum() != lastAckStateNum) {
      return;
    }
    if (server.equals(currentView.primary())) {
      if (currentView.backup() != null) {
        viewNum++;
        currentView = new View(viewNum, currentView.backup(), null);
        getNewBackup();
      }
    } else if (server.equals(currentView.backup())) {
      viewNum++;
      currentView = new View(viewNum, currentView.primary(), null);
      getNewBackup();
    }
    mostRecentlyPinged.remove(server);
  }

  private void getNewUpdate() {

    if (currentView.viewNum() != lastAckStateNum) {
      return;
    }
    if (currentView.primary() == null) {
      for (Address server : mostRecentlyPinged) {
        if (currentView.viewNum() == STARTUP_VIEWNUM || server.equals(currentView.backup())) {
          viewNum++;
          currentView = new View(viewNum, server, null);
          break;
        }
      }

    } else if (currentView.backup() == null && currentView.viewNum() == lastAckStateNum) {
      for (Address server : mostRecentlyPinged) {
        if (!server.equals(currentView.primary())) {
          viewNum++;
          currentView = new View(viewNum, currentView.primary(), server);
          break;
        }
      }
    }
  }

  public void getNewBackup() {
    // System.out.println( ActiveServers);
    for (Address server : ActiveServers) {
      if (!server.equals(currentView.primary())) {
        currentView = new View(currentView.viewNum(), currentView.primary(), server);
      }
    }
  }
}

// alternate

// package dslabs.primarybackup;
//
// import static dslabs.primarybackup.PingCheckTimer.PING_CHECK_MILLIS;
//
// import dslabs.framework.Address;
// import dslabs.framework.Node;
// import java.util.ArrayList;
// import java.util.HashMap;
// import java.util.HashSet;
// import java.util.List;
// import java.util.Map;
// import java.util.Objects;
// import java.util.Queue;
// import java.util.Set;
// import javax.swing.text.StyledEditorKit.BoldAction;
// import lombok.EqualsAndHashCode;
// import lombok.ToString;
//
// @ToString(callSuper = true)
// @EqualsAndHashCode(callSuper = true)
// class ViewServer extends Node {
//  static final int STARTUP_VIEWNUM = 0;
//  private static final int INITIAL_VIEWNUM = 1;
//
//  // Your code here...
//  private View currentView;
//  private int viewNum;
//  private int lastAckStateNum;
//  private Address client;
//  private HashSet<Address> mostRecentlyPinged;
//  private HashSet<Address> ActiveServers;
//
//  /*
// -----------------------------------------------------------------------------------------------
//   *  Construction and Initialization
//   *
// ---------------------------------------------------------------------------------------------*/
//  public ViewServer(Address address) {
//    super(address);
//  }
//
//  @Override
//  public void init() {
//    set(new PingCheckTimer(), PING_CHECK_MILLIS);
//    // Your code here...
//    currentView = new View(STARTUP_VIEWNUM, null, null);
//    lastAckStateNum = STARTUP_VIEWNUM;
//    viewNum = 1;
//    mostRecentlyPinged = new HashSet<>();
//    ActiveServers = new HashSet<>();
//    client = null;
//  }
//
//  /*
// -----------------------------------------------------------------------------------------------
//   *  Message Handlers
//   *
// ---------------------------------------------------------------------------------------------*/
//  private void handlePing(Ping m, Address sender) {
//    // Your code here...
//    // System.out.println( "handle ping"+mostRecentlyPinged);
//
//    mostRecentlyPinged.add(sender);
//
//    if (currentView.viewNum() == STARTUP_VIEWNUM) {
//      currentView = new View(INITIAL_VIEWNUM, sender, null);
//      //System.out.println( "View is updated "+currentView);
//
//    }
//    else if (sender.equals(currentView.primary()) && m.viewNum() == currentView.viewNum()) {
//      lastAckStateNum = currentView.viewNum();
//    }
//
//    getNewUpdate();
//    send(new ViewReply(currentView), sender);
//  }
//
//  private void handleGetView(GetView m, Address sender) {
//    // Your code here...
//    send(new ViewReply(currentView),sender);
//    client=sender;
//
//  }
//
//  /*
// -----------------------------------------------------------------------------------------------
//   *  Timer Handlers
//   *
// ---------------------------------------------------------------------------------------------*/
//  private void onPingCheckTimer(PingCheckTimer t) {
//    //ActiveServers = new HashSet<>(mostRecentlyPinged);
//    Address invalidServer = null;
//    for(Address aliveServer : ActiveServers) {
//      if(!mostRecentlyPinged.contains(aliveServer)) {
//        invalidServer = aliveServer;
//      }
//    }
//    if(invalidServer != null) {
//      ActiveServers.remove(invalidServer);
//      getValidState(invalidServer);
//    }
//
//    for(Address address : mostRecentlyPinged){
//      ActiveServers.add(address);
//    }
//    mostRecentlyPinged.clear();
//
//    getNewUpdate();
//    set(t, PING_CHECK_MILLIS);
//  }
//
//  /*
// -----------------------------------------------------------------------------------------------
//   *  Utils
//   *
// ---------------------------------------------------------------------------------------------*/
//  // Your code here...
//
//  private void getValidState(Address server) {
//    if (currentView.viewNum() != lastAckStateNum) {
//      return;
//    }
//    if (server.equals(currentView.primary())) {
//      if (currentView.backup() != null) {
//        viewNum++;
//        //System.out.println(viewNum);
//        currentView = new View(viewNum, currentView.backup(), null);
//        getNewBackup();
//      }
//    }
//    else if (server.equals(currentView.backup())) {
//      viewNum++;
//      currentView = new View(viewNum, currentView.primary(), null);
//      getNewBackup();
//    }
//    mostRecentlyPinged.remove(server);
//  }
//
//  private void getNewUpdate() {
//
//    if (currentView.viewNum() != lastAckStateNum) {
//      return;
//    }
//    if (currentView.primary() == null) {
//      for (Address server : mostRecentlyPinged) {
//        if (currentView.viewNum() == STARTUP_VIEWNUM || server.equals(currentView.backup())) {
//          viewNum++;
//          currentView = new View(viewNum, server, null);
//          //System.out.println( "View is updated line 129"+currentView);
//          break;
//        }
//      }
//
//    }
//    else if (currentView.backup() == null && currentView.viewNum() == lastAckStateNum) {
//      for (Address server : mostRecentlyPinged) {
//        if (!server.equals(currentView.primary()) ) {
//          viewNum++;
//          currentView = new View(viewNum, currentView.primary(), server);
//          break;
//        }
//      }
//    }
//  }
//  public void getNewBackup(){
//    // System.out.println( ActiveServers);
//    for(Address server: ActiveServers){
//      if(!server.equals(currentView.primary()) ){
//        currentView = new View(currentView.viewNum(), currentView.primary(), server);
//      }
//    }
//  }
//
// }
