package gobblin.util;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class MultiTaskId {
  private static Pattern TASK_ID_PATTERN = Pattern.compile("multitask_(.*)_(\\d+)");

  @Getter
  private String taskName;

  @Getter
  private String sequence;

  @Override
  public String toString() {
    return String.format("multitask_%s_%s", taskName, sequence);
  }

  public static MultiTaskId create(String taskName, int sequence) {
    return new MultiTaskId(taskName, Integer.toString(sequence));
  }

  public static MultiTaskId parse(String taskId) {
    Matcher matcher = TASK_ID_PATTERN.matcher(taskId);
    if (matcher.find()) {
      return new MultiTaskId(matcher.group(1), matcher.group(2));
    }
    throw new RuntimeException("Invalid multitask_ id: " + taskId);
  }
}
