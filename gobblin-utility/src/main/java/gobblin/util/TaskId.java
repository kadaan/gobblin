package gobblin.util;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class TaskId {
  private static Pattern TASK_ID_PATTERN = Pattern.compile("task_(.*)_(\\d+)");

  @Getter
  private String taskName;

  @Getter
  private String sequence;

  @Override
  public String toString() {
    return String.format("task_%s_%s", taskName, sequence);
  }

  public static TaskId create(String taskName, int sequence) {
    return new TaskId(taskName, Integer.toString(sequence));
  }

  public static TaskId parse(String taskId) {
    Matcher matcher = TASK_ID_PATTERN.matcher(taskId);
    if (matcher.find()) {
      return new TaskId(matcher.group(1), matcher.group(2));
    }
    throw new RuntimeException("Invalid task id: " + taskId);
  }
}
