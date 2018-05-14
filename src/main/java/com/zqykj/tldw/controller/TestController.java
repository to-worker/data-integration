package com.zqykj.tldw.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author feng.wei
 * @date 2018/5/14
 */
@RestController
@RequestMapping(value = "/api/v1/")
public class TestController {

    @RequestMapping(value = "test", method = RequestMethod.GET)
    public String testQuery() {
        return "successful..";
    }

}
