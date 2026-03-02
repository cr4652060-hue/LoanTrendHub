package com.example.loantrendhub.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

/** 确保根路径永远返回首页（避免刷新 404） */
@Controller
public class HomeController {
    @GetMapping("/")
    public String home() {
        return "forward:/index.html";
    }
}
