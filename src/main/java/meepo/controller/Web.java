package meepo.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * Created by peiliping on 17-2-21.
 */

@Controller public class Web {

    @RequestMapping("/") @ResponseBody public String index() {
        return "Hello World!";
    }

}
