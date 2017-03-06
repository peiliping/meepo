package meepo.transform.task;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import meepo.transform.config.TaskContext;
import meepo.util.Util;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by peiliping on 17-3-6.
 */
@Component public class TasksManager {

    protected static final Logger LOG = LoggerFactory.getLogger(TasksManager.class);

    private ConcurrentMap<String, Pair<TaskContext, Task>> container = Maps.newConcurrentMap();

    private ThreadPoolExecutor selfMonitor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());

    public TasksManager() {
        this.selfMonitor.submit(() -> {
            while (true) {
                Util.sleep(60);
                List<String> keys = Lists.newArrayList(container.keySet());
                keys.forEach(s -> container.get(s).getRight().checkSourcesFinished());
            }
        });
    }

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

    public List<TaskContext> listTasks() {
        List<TaskContext> result = Lists.newArrayList();
        List<Pair<TaskContext, Task>> t = Lists.newArrayList(this.container.values());
        t.forEach(item -> result.add(item.getLeft()));
        return result;
    }

    public boolean forceStopTask(String taskName) {
        if (!this.container.containsKey(taskName)) {
            return false;
        }
        Task task = this.container.get(taskName).getRight();
        try {
            task.close();
            this.container.remove(taskName);
        } catch (Exception e) {
            LOG.error("Close Task [" + taskName + "] Failed : ", e);
            return false;
        }
        return true;
    }

}
