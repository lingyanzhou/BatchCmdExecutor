package lzhou.batch_cmd_executor.configuration;

import lzhou.batch_cmd_executor.domain.CmdInput;
import lzhou.batch_cmd_executor.domain.ExecResult;
import lzhou.batch_cmd_executor.partitioner.HashPartitioner;
import lzhou.batch_cmd_executor.processor.FilterByHashItemProcessor;
import lzhou.batch_cmd_executor.processor.ProcessExecProcessor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.batch.item.support.CompositeItemProcessor;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.validation.BindException;

import java.util.Arrays;

/**
 * Created by zhoulingyan on 2/16/18.
 */
@Configuration
public class BatchConfiguration {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Value("${timeoutSeconds}")
    private long timeoutSeconds;

    @Value("${normalExitStatus}")
    private int normalExitStatus;

    @Value("${gridSize}")
    private int gridSize;

    @Bean
    @StepScope
    public FlatFileItemReader<CmdInput> cmdInputReader(@Value("#{jobParameters[inPath]}") String inPath) {
        FlatFileItemReader<CmdInput> reader = new FlatFileItemReader<>();
        DefaultLineMapper mapper = new DefaultLineMapper();
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        tokenizer.setDelimiter(",");
        mapper.setLineTokenizer(tokenizer);
        mapper.setFieldSetMapper(fieldSet -> new CmdInput(fieldSet.readString(0), fieldSet.readString(1)));
        reader.setResource(new FileSystemResource(inPath));
        reader.setLineMapper(mapper);
        return reader;
    }

    @Bean
    @StepScope
    public FlatFileItemWriter<ExecResult> writer(@Value("#{jobParameters[outPath]}") String outPath) {
        FlatFileItemWriter<ExecResult> writer = new FlatFileItemWriter<>();
        DelimitedLineAggregator<ExecResult> aggregator = new DelimitedLineAggregator<>();
        BeanWrapperFieldExtractor<ExecResult> extractor = new BeanWrapperFieldExtractor<>();
        extractor.setNames(new String[]{"cmdGroup","cmd","exitCode","exception","startTimeStamp","endTimeStamp","milliSecElapsed"});
        aggregator.setDelimiter(",");
        aggregator.setFieldExtractor(extractor);
        writer.setShouldDeleteIfExists(false);
        writer.setAppendAllowed(true);
        writer.setResource(new FileSystemResource(outPath));
        writer.setLineSeparator("\n");
        writer.setLineAggregator(aggregator);
        return writer;
    }

    @Bean
    @StepScope
    public ProcessExecProcessor processExecProcessor() {
        return new ProcessExecProcessor(timeoutSeconds, normalExitStatus);
    }

    @Bean
    @StepScope
    public FilterByHashItemProcessor filterByHashItemProcessor() {
        return new FilterByHashItemProcessor();
    }

    @Bean
    public Step step1Child(ItemReader<CmdInput> cmdInputReader,
                           ProcessExecProcessor processExecProcessor,
                           FilterByHashItemProcessor filterByHashItemProcessor,
                           ItemWriter<ExecResult> writer
                           ) {
        CompositeItemProcessor<CmdInput, ExecResult> compProcessor = new CompositeItemProcessor<>();
        compProcessor.setDelegates(Arrays.asList(filterByHashItemProcessor, processExecProcessor));
        return stepBuilderFactory.get("step1Child")
                .<CmdInput, ExecResult>chunk(10)
                .reader(cmdInputReader)
                .processor(compProcessor)
                .writer(writer)
                .listener(filterByHashItemProcessor)
                .build();
    }

    @Bean
    public Step step1(Step step1Child) {
        SimpleAsyncTaskExecutor simpleAsyncTaskExecutor = new SimpleAsyncTaskExecutor();
        simpleAsyncTaskExecutor.setConcurrencyLimit(gridSize);
        return stepBuilderFactory.get("step1")
                .partitioner(step1Child)
                .partitioner("step1Child", new HashPartitioner())
                .taskExecutor(simpleAsyncTaskExecutor)
                .build();
    }

    @Bean
    public Job job(Step step1) throws Exception {
        return jobBuilderFactory.get("job1")
                .incrementer(new RunIdIncrementer())
                .start(step1)
                .build();
    }
}