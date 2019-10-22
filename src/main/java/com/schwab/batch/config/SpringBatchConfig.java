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
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.FixedLengthTokenizer;
import org.springframework.batch.item.file.transform.LineTokenizer;
import org.springframework.batch.item.file.transform.Range;
import org.springframework.batch.item.xml.StaxEventItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.oxm.jaxb.Jaxb2Marshaller;


@Configuration
@EnableBatchProcessing
public class SpringBatchConfig {

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Value("inputflat.dat")
    private Resource inputFlatFile;

    @Value("file:person3.xml")
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
    public FlatFileItemReader<Person> fileItemReader() {
        FlatFileItemReader<Person> fileItemReader = new FlatFileItemReader<>();
        fileItemReader.setResource(inputFlatFile);
        fileItemReader.setLineMapper(personLineMapper());
        return fileItemReader;
    }

    @Bean
    public LineMapper<Person> personLineMapper() {
        DefaultLineMapper<Person> mapper = new DefaultLineMapper<Person>();
        mapper.setLineTokenizer(personLineTokenizer());
        mapper.setFieldSetMapper(new PersonFieldSetMapper());
        return mapper;
    }

    @Bean
    public LineTokenizer personLineTokenizer() {
        FixedLengthTokenizer tokenizer = new FixedLengthTokenizer();
        tokenizer.setColumns(new Range[]{new Range(1, 5), new Range(6, 16), new Range(17, 29)});
        tokenizer.setNames(new String[]{"id", "firstName", "lastName"});
        return tokenizer;
    }


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
