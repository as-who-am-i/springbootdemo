package com.example.springbootdemo;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @Prigram: com.example.springbootdemo
 * @Description: TODO
 * @Author: DongFang
 * @CreaeteTime: 2018-10-12 21:51
 */

@RestController
public class HelloController {

    @RequestMapping("/hello")
    public String hello() {
        return "hello,springboot！";
    }
}


