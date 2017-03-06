package meepo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication public class DataTransformApplication {

    public static void main(String[] args) {
        ConfigurableApplicationContext cac = SpringApplication.run(DataTransformApplication.class, args);
    }
}
