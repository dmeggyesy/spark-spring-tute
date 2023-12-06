package li.zah.sparkspringtuteapi.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

public class SparkSpringTuteApiConfig {
  @Bean
  @Primary
  public ObjectMapper mapper() {
    return new ObjectMapper().findAndRegisterModules().configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
  }

}
