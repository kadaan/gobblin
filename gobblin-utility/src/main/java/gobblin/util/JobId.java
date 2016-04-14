package gobblin.util;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

@AllArgsConstructor
public class JobId{
  private static Pattern JOB_ID_PATTERN = Pattern.compile("job_(.*)_(\\d+)");

  @Getter
  private String jobName;

  @Getter
  private String sequence;

  @Override
  public String toString() {
    return String.format("job_%s_%s", jobName, sequence);
  }

  public static JobId parse(String jobId) {
    Matcher matcher = JOB_ID_PATTERN.matcher(jobId);
    if (matcher.find()) {
      return new JobId(matcher.group(1), matcher.group(2));
    }
    throw new RuntimeException("Invalid job id: " + jobId);
  }
}
