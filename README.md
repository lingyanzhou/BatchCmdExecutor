# Batch Cmd Executor

The batch can be use to execute a batch of cmd commands in parallel, and records the results of each one into a csv file.

## Job Parameters

  inPath: The path of the input csv, containing all cmds to execute, in the format `[cmdGroup],[cmd]`. 
  outPath: The path of the output csv, containing all execution results, in the format `[cmdGroup],[cmd]`. 
  
## Use external configuration

```bash
java -Dspring.config.location=./config -Djava.security.egd=/dev/urandom -jar batch_cmd_executor.jar inPath=in.csv outPath=out.csv
```