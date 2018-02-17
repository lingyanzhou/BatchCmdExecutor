package lzhou.batch_cmd_executor.partitioner;

import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by zhoulingyan on 2/16/18.
 */
public class HashPartitioner implements Partitioner {
    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        Map<String, ExecutionContext> result
                = new HashMap<String, ExecutionContext>();

        for (int i = 1; i <= gridSize; i++) {
            ExecutionContext value = new ExecutionContext();

            value.putInt("threadId", i);
            value.putInt("gridSize", gridSize);

            result.put("partition" + i, value);
        }

        return result;
    }
}
