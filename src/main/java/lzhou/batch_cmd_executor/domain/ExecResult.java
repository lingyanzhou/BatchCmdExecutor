package lzhou.batch_cmd_executor.domain;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Created by zhoulingyan on 2/16/18.
 */
@Data
@AllArgsConstructor
public class ExecResult {
    private String cmdGroup;
    private String cmd;
    private int exitCode;
    private String exception;
    private long startTimeStamp;
    private long endTimeStamp;
    private long milliSecElapsed;
}
