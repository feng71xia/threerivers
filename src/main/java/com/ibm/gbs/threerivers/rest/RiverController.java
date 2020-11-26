package com.ibm.gbs.threerivers.rest;

import com.ibm.gbs.threerivers.kafka.ThreeRiversDemoKTable;
import com.ibm.gbs.threerivers.kafka.ThreeRiversDemoGlobalKTable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/")
public class RiverController {
    private ThreeRiversDemoKTable demoKTable;
    private ThreeRiversDemoGlobalKTable demoGlobalKTable;

    @GetMapping("/")
    public String help() {
        return "/demoKTable or /demoGlobalKTable or /demoKTableStop or /demoGlobalKTableStop";
    }

    @GetMapping("/{command}")
    public String findOne(@PathVariable String command) {
        if ("demoKTable".equalsIgnoreCase(command)) {
            demoKTable.start();
            return "demoKTable started";
        } else if ("demoGlobalKTable".equalsIgnoreCase(command)) {
            demoGlobalKTable.start();
            return "demoGlobalKTable started";
        } else if ("demoKTableStop".equalsIgnoreCase(command)) {
            demoKTable.stop();
            return "demoKTable stopped";
        } else if ("demoGlobalKTableStop".equalsIgnoreCase(command)) {
            demoGlobalKTable.stop();
            return "demoGlobalKTable stopped";
        } else {
            return "Give me something useful";
        }
    }

    @Autowired
    public void setThreeRiversDemoKTable(ThreeRiversDemoKTable val) {
        demoKTable = val;
    }

    @Autowired
    public void setThreeRiversDemoGlobalKTable(ThreeRiversDemoGlobalKTable val) {
        demoGlobalKTable = val;
    }

}
