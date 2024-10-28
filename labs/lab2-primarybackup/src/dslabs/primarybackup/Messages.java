package dslabs.primarybackup;

import dslabs.atmostonce.AMOApplication;
import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Message;
import lombok.Data;

/* -----------------------------------------------------------------------------------------------
 *  ViewServer Messages
 * ---------------------------------------------------------------------------------------------*/
@Data
class Ping implements Message {
  private final int viewNum;
}

@Data
class GetView implements Message {}

@Data
class ViewReply implements Message {
  private final View view;
}

/* -----------------------------------------------------------------------------------------------
 *  Primary-Backup Messages
 * ---------------------------------------------------------------------------------------------*/
@Data
class Request implements Message {
  // Your code here...
  private final AMOCommand command;
  private final View currentView;
}

@Data
class Reply implements Message {
  // Your code here...
  private final AMOResult result;
  private final boolean isError;
}

@Data
class StateTransfer implements Message {
  // Your code here...
  private final AMOApplication<Application> amoApplication;
  private final Address address;
  private final View view;
}

// Your code here...

@Data
class ForwardedRequest implements Message {
  // Your code here...
  private final Request request;
  private final Address address;
  private final View view;
}

@Data
class AckMessageST implements Message {
  // Your code here...
  private final View view;
  private final boolean ack;
}

@Data
class AckMessageFR implements Message {
  // Your code here...
  private final boolean ack;
  private final int seqNum;
}
