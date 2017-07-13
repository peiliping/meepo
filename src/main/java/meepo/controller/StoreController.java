package meepo.controller;

import meepo.storage.BitStore;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.Set;

/**
 * Created by peiliping on 17-7-13.
 */
@Controller
public class StoreController {

    @RequestMapping("/task/bitresult/list")
    @ResponseBody
    public Set<String> list() {
        return BitStore.getInstance().list();
    }

    @RequestMapping("/task/bitresult/diff/{s1}/{s2}")
    @ResponseBody
    public String diff(String s1, String s2) {
        return BitStore.getInstance().diff(s1, s2);
    }

    @RequestMapping("/task/bitresult/count/{s1}")
    @ResponseBody
    public int count(String s1) {
        return BitStore.getInstance().getSize(s1);
    }

    @RequestMapping("/task/bitresult/clean/{s1}")
    @ResponseBody
    public String clean(String s1) {
        BitStore.getInstance().clean(s1);
        return "OK";
    }
}
