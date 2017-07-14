package meepo.controller;

import com.google.common.collect.Lists;
import meepo.transform.config.TaskContext;
import meepo.transform.report.IReportItem;
import meepo.transform.task.TasksManager;
import meepo.util.Constants;
import org.apache.commons.lang3.Validate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.io.IOException;
import java.util.List;

/**
 * Created by peiliping on 17-2-21.
 */

@Controller
public class TaskController {

    @Autowired
    private Environment env;

    @Autowired
    private TasksManager tasksManager;

    @RequestMapping("/")
    @ResponseBody
    public String index() {
        return "Hello World!";
    }

    @RequestMapping("/task/preload")
    @ResponseBody
    public List<TaskContext> checkTasks() throws Exception {
        TaskContext context = initTasksContext();
        List<String> taskNames = Lists.newArrayList(context.get(Constants.PROJECT_NAME).split("\\s"));
        List<TaskContext> taskConfigs = Lists.newArrayList();
        taskNames.forEach(taskName -> taskConfigs.add(new TaskContext(taskName, context.getSubProperties(Constants.PROJECT_NAME_ + taskName + "."))));
        return taskConfigs;
    }

    @RequestMapping("/task/list")
    @ResponseBody
    public List<TaskContext> listTasks() throws Exception {
        return this.tasksManager.listTasks();
    }

    @RequestMapping("/task/{taskName}/run")
    @ResponseBody
    public Boolean runTask(@PathVariable String taskName) throws Exception {
        Validate.notBlank(taskName);
        TaskContext context = initTasksContext();
        List<String> taskNames = Lists.newArrayList(context.get(Constants.PROJECT_NAME).split("\\s"));
        Validate.isTrue(taskNames.contains(taskName));
        TaskContext tc = new TaskContext(taskName, context.getSubProperties(Constants.PROJECT_NAME_ + taskName + "."));
        return this.tasksManager.addTask(tc);
    }

    @RequestMapping("/task/{taskName}/report")
    @ResponseBody
    public List<IReportItem> reportTask(@PathVariable String taskName) throws Exception {
        Validate.notBlank(taskName);
        return this.tasksManager.report(taskName);
    }

    @RequestMapping("/task/{taskName}/kill")
    @ResponseBody
    public Boolean killTask(@PathVariable String taskName) throws Exception {
        Validate.notBlank(taskName);
        TaskContext context = initTasksContext();
        List<String> taskNames = Lists.newArrayList(context.get(Constants.PROJECT_NAME).split("\\s"));
        Validate.isTrue(taskNames.contains(taskName));
        return this.tasksManager.forceStopTask(taskName);
    }

    private TaskContext initTasksContext() throws IOException {
        String configPath = env.getProperty("tasks.configuration.path");
        Validate.notNull(configPath);
        return new TaskContext(Constants.PROJECT_NAME, configPath);
    }

}
