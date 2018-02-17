package lzhou.batch_cmd_executor.domain;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Created by zhoulingyan on 2/16/18.
 */
@Data
@AllArgsConstructor
public class CmdInput {
    private String cmdGroup;
    private String cmd;
}
