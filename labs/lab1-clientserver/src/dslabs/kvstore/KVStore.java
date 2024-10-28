package dslabs.kvstore;

import dslabs.framework.Application;
import dslabs.framework.Command;
import dslabs.framework.Result;
import java.util.HashMap;
import java.util.Map;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.ToString;

@ToString
@EqualsAndHashCode
public class KVStore implements Application {

  public interface KVStoreCommand extends Command {}

  public interface SingleKeyCommand extends KVStoreCommand {
    String key();
  }

  @Data
  public static final class Get implements SingleKeyCommand {
    @NonNull private final String key;

    @Override
    public boolean readOnly() {
      return true;
    }
  }

  @Data
  public static final class Put implements SingleKeyCommand {
    @NonNull private final String key, value;
  }

  @Data
  public static final class Append implements SingleKeyCommand {
    @NonNull private final String key, value;
  }

  public interface KVStoreResult extends Result {}

  @Data
  public static final class GetResult implements KVStoreResult {
    @NonNull private final String value;
  }

  @Data
  public static final class KeyNotFound implements KVStoreResult {}

  @Data
  public static final class PutOk implements KVStoreResult {}

  @Data
  public static final class AppendResult implements KVStoreResult {
    @NonNull private final String value;
  }

  // Your code here...
  Map<String, String> dataStore = new HashMap<>();

  @Override
  public KVStoreResult execute(Command command) {
    if (command instanceof Get) {
      Get g = (Get) command;
      // Your code here...
      String key = g.key();
      if (dataStore.containsKey(key)) return new GetResult(dataStore.get(key));
      else return new KeyNotFound();
    }

    if (command instanceof Put) {
      Put p = (Put) command;
      // Your code here...
      dataStore.put(p.key(), p.value());
      return new PutOk();
    }

    if (command instanceof Append) {
      Append a = (Append) command;
      // Your code here...
      String key = a.key(), value = a.value();
      String prevValue = !dataStore.containsKey(key) ? "" : dataStore.get(key);
      StringBuilder sb = new StringBuilder();
      sb.append(prevValue);
      sb.append(value);
      String newValue = sb.toString();

      dataStore.put(key, newValue);
      return new AppendResult(newValue);
    }

    throw new IllegalArgumentException();
  }
}
