package nk.gk.wyl.elasticsearch.config;

import org.springframework.boot.web.servlet.MultipartConfigFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.servlet.MultipartConfigElement;
import java.io.File;

// @Configuration
public class FileConfig {


    /**
     * 解决
     Failed to parse multipart servlet request; nested exception is java.io.IOException: org.apache.tomcat.util.http.fileupload.FileUploadBase$IOFileUploadException: Processing of multipart/form-data request failed. Unexpected EOF read on the socket
     * @return
     */
    @Bean
    public MultipartConfigElement multipartConfigElement() {
        MultipartConfigFactory factory = new MultipartConfigFactory();
        System.out.println("-----------------------");
        String location = System.getProperty("user.dir") + "/data/tmp";
        System.out.println(location);
        File tmpFile = new File(location);
        if (!tmpFile.exists()) {
            tmpFile.mkdirs();
        }
        factory.setLocation(location);
        return factory.createMultipartConfig();
    }
}
