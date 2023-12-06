package li.zah.sparkspringtuteapi.config;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SparkConfig {
  @Value("${spark.app.name}")
  private String appName;

  @Value("${spark.master}")
  private String masterUri;

  @Bean
  public SparkConf conf() {

    String[] arr = new String[1];
    //  arr[0] = "./build/libs/spark-tute-0.0.1-SNAPSHOT.jar";
    SparkConf conf = new SparkConf().setAppName(appName).setMaster(masterUri)
        //     .setJars(arr);
        ;
    conf.validateSettings();
    return conf;
  }

  @Bean
  public JavaSparkContext sparkContext() {
    return new JavaSparkContext(conf());
  }
}
