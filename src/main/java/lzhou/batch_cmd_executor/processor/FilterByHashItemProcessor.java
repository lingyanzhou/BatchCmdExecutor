package lzhou.batch_cmd_executor.processor;

import lzhou.batch_cmd_executor.domain.CmdInput;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;

/**
 * Created by zhoulingyan on 2/16/18.
 */
public class FilterByHashItemProcessor implements ItemProcessor<CmdInput, CmdInput> {
    private int threadId;
    private int gridSize;

    public FilterByHashItemProcessor() {
        this.threadId = 0;
        this.gridSize = 0;
    }

    @BeforeStep
    void beforeStep(StepExecution stepExecution) {
        ExecutionContext executionContext = stepExecution.getExecutionContext();
        this.threadId = executionContext.getInt("threadId");
        this.gridSize = executionContext.getInt("gridSize");
    }

    @Override
    public CmdInput process(CmdInput cmdInput) throws Exception {
        int hashCode = cmdInput.hashCode();
        if (((hashCode % gridSize) + gridSize % gridSize) == threadId) {
            return cmdInput;
        } else {
            return null;
        }
    }
}
