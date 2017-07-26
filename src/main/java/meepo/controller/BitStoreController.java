package meepo.controller;

import meepo.util.hp.bit.Bit64Store;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.Set;

/**
 * Created by peiliping on 17-7-13.
 */
@Controller
public class BitStoreController {

    @RequestMapping("/task/bitstore/keys")
    @ResponseBody
    public Set<String> keys() {
        return Bit64Store.getInstance().keys();
    }

    @RequestMapping("/task/bitstore/diff/{s1}/{s2}")
    @ResponseBody
    public String diff(@PathVariable String s1, @PathVariable String s2) {
        return Bit64Store.getInstance().diff(s1, s2);
    }

    @RequestMapping("/task/bitstore/count/{s1}")
    @ResponseBody
    public long count(@PathVariable String s1) {
        return Bit64Store.getInstance().getSize(s1);
    }

    @RequestMapping("/task/bitresult/clean/{s1}")
    @ResponseBody
    public String clean(@PathVariable String s1) {
        Bit64Store.getInstance().clean(s1);
        return "OK";
    }
}
