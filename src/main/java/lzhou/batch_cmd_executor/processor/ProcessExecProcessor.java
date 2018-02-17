package lzhou.batch_cmd_executor.processor;

import lzhou.batch_cmd_executor.domain.CmdInput;
import lzhou.batch_cmd_executor.domain.ExecResult;
import org.apache.commons.exec.*;
import org.springframework.batch.item.ItemProcessor;

import java.util.List;

/**
 * Created by zhoulingyan on 2/16/18.
 */
public class ProcessExecProcessor implements ItemProcessor<CmdInput, ExecResult> {
    private long timeoutSeconds;

    private int normalExitStatus;

    public ProcessExecProcessor(long timeoutSeconds, int normalExitStatus) {
        this.timeoutSeconds = timeoutSeconds;
        this.normalExitStatus = normalExitStatus;
    }

    @Override
    public ExecResult process(CmdInput cmdInput) throws Exception {
        if (cmdInput.getCmd().length()==0) {
            return null;
        }
        CommandLine commandLine = CommandLine.parse(cmdInput.getCmd());

        long  startExecTime = System.currentTimeMillis();
        DefaultExecutor executor = new DefaultExecutor();
        DefaultExecuteResultHandler resultHandler = new DefaultExecuteResultHandler();
        ExecuteWatchdog watchdog = new ExecuteWatchdog(timeoutSeconds*1000);

        executor.setExitValue(normalExitStatus);
        executor.setWatchdog(watchdog);
        executor.execute(commandLine, resultHandler);

        try {
            resultHandler.waitFor();
            long  endExecTime = System.currentTimeMillis();
            return new ExecResult(cmdInput.getCmdGroup(), cmdInput.getCmd(), resultHandler.getExitValue(), resultHandler.getException().getMessage(),
                    startExecTime, endExecTime, endExecTime - startExecTime);
        } catch (Exception e) {
            long  endExecTime = System.currentTimeMillis();
            return new ExecResult(cmdInput.getCmdGroup(), cmdInput.getCmd(), Integer.MIN_VALUE, e.getMessage(),
                    startExecTime, endExecTime, endExecTime - startExecTime);
        }
    }
}
