package com.example.pulsar;

import com.example.pulsar.annotation.EnablePulsar;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
@EnablePulsar
public class Application {
    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }
}
