package dslabs.atmostonce;

import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Command;
import dslabs.framework.Result;
import java.util.HashMap;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@EqualsAndHashCode
@ToString
@RequiredArgsConstructor
public final class AMOApplication<T extends Application> implements Application {
  @Getter @NonNull private final T application;

  // Your code here...
  private final Map<Address, AMOResult> cache = new HashMap<Address, AMOResult>();
  private Address hashcode;

  @Override
  public AMOResult execute(Command command) {
    if (!(command instanceof AMOCommand)) {
      throw new IllegalArgumentException();
    }

    AMOCommand amoCommand = (AMOCommand) command;
    this.hashcode = amoCommand.address();
    // Your code here...
    //    System.out.println("Got it " + command);
    if (alreadyExecuted(amoCommand)) return this.cache.get(this.hashcode);

    AMOResult Result =
        new AMOResult(application.execute(amoCommand.command()), amoCommand.sequenceNum());
    this.cache.put(this.hashcode, Result);
    return Result;
  }

  public Result executeReadOnly(Command command) {
    while (!command.readOnly()) {
      throw new IllegalArgumentException();
    }

    if (command instanceof AMOCommand) {
      return execute(command);
    }

    return application.execute(command);
  }

  public boolean alreadyExecuted(AMOCommand amoCommand) {
    // Your code here...
    if (this.cache.containsKey(this.hashcode)
        && this.cache.get(this.hashcode).sequenceNum() >= amoCommand.sequenceNum()) return true;
    return false;
  }
}
