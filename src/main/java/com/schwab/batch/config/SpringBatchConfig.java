package com.schwab.batch.config;

import com.schwab.batch.listener.JobCompletionNotificationListener;
import com.schwab.batch.mapper.PersonFieldSetMapper;
import com.schwab.batch.model.Person;
import com.schwab.batch.processor.PersonItemProcessor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.FixedLengthTokenizer;
import org.springframework.batch.item.file.transform.LineTokenizer;
import org.springframework.batch.item.file.transform.PatternMatchingCompositeLineTokenizer;
import org.springframework.batch.item.file.transform.Range;
import org.springframework.batch.item.xml.StaxEventItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.oxm.jaxb.Jaxb2Marshaller;

import java.util.HashMap;


@Configuration
@EnableBatchProcessing
public class SpringBatchConfig {

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Value("input1.dat")
    private Resource inputFlatFile;

    @Value("file:person4.xml")
    private Resource outputXml;

    @Bean
    public Person person() {
        return new Person();
    }

    @Bean
    public ItemProcessor<Person, Person> itemProcessor() {
        return new PersonItemProcessor();
    }


    @Bean
    public JobLauncher jbLauncher(JobRepository jobRepository) {
        SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
        jobLauncher.setJobRepository(jobRepository);
        return jobLauncher;
    }

    @Bean
    public Jaxb2Marshaller jaxb2Marshaller() {
        Jaxb2Marshaller jaxb2Marshaller = new Jaxb2Marshaller();
        jaxb2Marshaller.setClassesToBeBound(Person.class);
        return jaxb2Marshaller;
    }

    /*@Bean
    public FlatFileItemReader<Person> fileItemReader() {
        FlatFileItemReader<Person> fileItemReader = new FlatFileItemReader<>();
        fileItemReader.setResource(new ClassPathResource("input.csv"));
        DelimitedLineTokenizer delimitedLineTokenizer = new DelimitedLineTokenizer();
        delimitedLineTokenizer.setNames("id", "firstName", "lastName");
        DefaultLineMapper<Person> defaultLineMapper = new DefaultLineMapper<>();
        defaultLineMapper.setLineTokenizer(delimitedLineTokenizer);
        defaultLineMapper.setFieldSetMapper(new PersonFieldSetMapper());
        fileItemReader.setLineMapper(defaultLineMapper);
        return fileItemReader;
    }*/

    @Bean
    public FlatFileItemReader<Person> fileItemReader(LineTokenizer lineTokenizer) {
        FlatFileItemReader<Person> fileItemReader = new FlatFileItemReader<>();
        fileItemReader.setResource(inputFlatFile);

        DefaultLineMapper<Person> mapper = new DefaultLineMapper<Person>();
        mapper.setLineTokenizer(lineTokenizer);

        mapper.setFieldSetMapper(new PersonFieldSetMapper());

        fileItemReader.setLineMapper(mapper);
        return fileItemReader;
    }

    @Bean
    public LineTokenizer fixedFileDescriptor() {
        PatternMatchingCompositeLineTokenizer rc = new PatternMatchingCompositeLineTokenizer();

        HashMap<String, LineTokenizer> matchers = new HashMap<>();
        matchers.put("RS*", submitterRecordTokenizer());
        //matchers.put("RA*", employerRecordTokenizer());
        //matchers.put("RT*", endRecordTokenizer());
        matchers.put("*", employeeRecordTokenizer());

        rc.setTokenizers(matchers);
        return rc;
    }

    private LineTokenizer employeeRecordTokenizer() {
        FixedLengthTokenizer tokenizer = new FixedLengthTokenizer();
        String[] names = new String[]{"recordType", "id", "firstName", "lastName", "line1", "line2", "line3"};
        tokenizer.setNames(names);
        Range[] ranges = new Range[]{
                new Range(1, 2),
                new Range(3, 7),
                new Range(8, 19),
                new Range(20, 32),
                new Range(33, 41),
                new Range(42, 49),
                new Range(50, 54)

        };

        // tokenizer.setColumns(new Range[]{new Range(1, 5), new Range(6, 16), new Range(17, 29)});
        //tokenizer.setNames(new String[]{"id", "firstName", "lastName"});
        tokenizer.setColumns(ranges);
        return tokenizer;
    }

    private LineTokenizer endRecordTokenizer() {
        FixedLengthTokenizer rc = new FixedLengthTokenizer();
        String[] names = new String[]{"count"};
        Range[] ranges = new Range[]{new Range(1, 3)};
        rc.setNames(names);
        rc.setColumns(ranges);
        return rc;
    }

    private LineTokenizer employerRecordTokenizer() {
        FixedLengthTokenizer rc = new FixedLengthTokenizer();
        String[] names = new String[]{"employerName"};
        Range[] ranges = new Range[]{new Range(1, 11)};
        rc.setNames(names);
        rc.setColumns(ranges);
        return rc;
    }

    private LineTokenizer submitterRecordTokenizer() {
        FixedLengthTokenizer rc = new FixedLengthTokenizer();
        String[] names = new String[]{"recordType", "id", "submitterName"};
        Range[] ranges = new Range[]{new Range(1, 2), new Range(3, 7), new Range(8, 14)};
        rc.setNames(names);
        rc.setColumns(ranges);
        return rc;
    }

    /*@Bean
    public LineTokenizer personLineTokenizer() {
        FixedLengthTokenizer tokenizer = new FixedLengthTokenizer();
        tokenizer.setColumns(new Range[]{new Range(1, 5), new Range(6, 16), new Range(17, 29)});
        tokenizer.setNames(new String[]{"id", "firstName", "lastName"});
        return tokenizer;
    }*/


    @Bean(destroyMethod = "")
    public StaxEventItemWriter<Person> staxEventItemWriter(Jaxb2Marshaller marshaller) {
        StaxEventItemWriter<Person> staxEventItemWriter = new StaxEventItemWriter<>();
        staxEventItemWriter.setResource(outputXml);
        staxEventItemWriter.setMarshaller(marshaller);
        staxEventItemWriter.setRootTagName("personInfo");
        return staxEventItemWriter;
    }

    @Bean
    public Job jobFlatFileXml(JobCompletionNotificationListener listener, Step step) {
        return jobBuilderFactory.get("jobFlatFileXml")
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .flow(step)
                .end()
                .build();
    }

    @Bean
    public Step step1(ItemReader<Person> reader, ItemWriter<Person> writer, ItemProcessor<Person, Person> processor) {
        return stepBuilderFactory.get("step1")
                .<Person, Person>chunk(2)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .build();
    }
}
