package nk.gk.wyl.elasticsearch.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;

@Configuration
@Order(10)
public class InitRunner {

    @Bean
    public int run(){
        return 0;
    }

}
