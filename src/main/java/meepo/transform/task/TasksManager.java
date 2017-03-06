package meepo.transform.task;

import com.google.common.collect.Maps;
import meepo.transform.config.TaskContext;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentMap;

/**
 * Created by peiliping on 17-3-6.
 */
@Component public class TasksManager {

    protected static final Logger LOG = LoggerFactory.getLogger(TasksManager.class);

    private ConcurrentMap<String, Pair<TaskContext, Task>> container = Maps.newConcurrentMap();

    public boolean addTask(TaskContext tc) {
        if (this.container.containsKey(tc.getTaskName())) {
            return false;
        }
        Object r = this.container.putIfAbsent(tc.getTaskName(), Pair.of(tc, new Task(tc.getTaskName())));
        if (r != null) {
            return false;
        }
        Task task = this.container.get(tc.getTaskName()).getRight();
        try {
            task.init(tc);
            task.start();
        } catch (Exception e) {
            LOG.error("Add Task [" + tc.getTaskName() + "] Failed : ", e);
            this.container.remove(tc.getTaskName());
            return false;
        }
        return true;
    }

    public boolean forceStopTask(String taskName) {
        if (!this.container.containsKey(taskName)) {
            return false;
        }
        Task task = this.container.get(taskName).getRight();
        try {
            task.close();
        } catch (Exception e) {
            LOG.error("Close Task [" + taskName + "] Failed : ", e);
            return false;
        }
        return true;
    }

}
