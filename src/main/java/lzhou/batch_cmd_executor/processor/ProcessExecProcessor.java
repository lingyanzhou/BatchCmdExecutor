package lzhou.batch_cmd_executor.processor;

import lzhou.batch_cmd_executor.domain.CmdInput;
import lzhou.batch_cmd_executor.domain.ExecResult;
import org.apache.commons.exec.*;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;

import java.util.List;

/**
 * Created by zhoulingyan on 2/16/18.
 */
public class ProcessExecProcessor implements ItemProcessor<CmdInput, ExecResult> {
    private static final Logger logger = LoggerFactory.getLogger(ProcessExecProcessor.class);
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
        try {
            executor.execute(commandLine, resultHandler);
            resultHandler.waitFor();
            long  endExecTime = System.currentTimeMillis();
            String errorMessage= resultHandler.getException() == null? null: resultHandler.getException().getMessage();
            return new ExecResult(cmdInput.getCmdGroup(), cmdInput.getCmd(), resultHandler.getExitValue(), errorMessage,
                    startExecTime, endExecTime, endExecTime - startExecTime);
        } catch (Exception e) {
            logger.warn(ExceptionUtils.getStackTrace(e));
            long  endExecTime = System.currentTimeMillis();
            return new ExecResult(cmdInput.getCmdGroup(), cmdInput.getCmd(), Integer.MIN_VALUE, e.getMessage(),
                    startExecTime, endExecTime, endExecTime - startExecTime);
        }
    }
}
