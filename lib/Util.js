'use strict'

const _ = require('lodash')

exports.registerTasks = function(profile, app, rabbit) {
  profile.tasks.forEach(taskName => {
    app.tasker.activeTasks.set(taskName, [])
    exports.registerRun(taskName, app, rabbit)
    exports.registerInterrupt(taskName, app, rabbit)
  })
}

exports.clearHandler = function (activeTasks, task) {
  // remove the task from the taskerClient handlers list
  _.remove(activeTasks, activeTask => {
    return task.id = activeTask.id
  })
}

exports.registerInterrupt = function (taskName, app, rabbit) {
  rabbit.handle(`${taskName}.interrupt`, message => {
    const taskId = message.body.taskId
    const activeTasks = app.tasker.activeTasks.get(taskName) || []
    const task = _.find(activeTasks, activeTask => {
      return activeTask.id = taskId
    })

    if (!task) {
      app.log.warn(`Failed to interrupt task, no active handler found for task ${taskName}  and id ${taskId}`)
      return message.reject()
    }

    task.interrupt(message)
  })
}

exports.registerRun = function (taskName, app, rabbit) {
  const taskerClient = app.tasker

  // set up the task handler
  rabbit.handle(taskName, message => {
    if (!app.api.tasks[taskName]) {
      app.log.error(`No task defined for task name: ${taskName}. Message body was: ${JSON.stringify(message.body)}`)
      return message.reject()
    }

    if (app.workerCount >= app.config.tasker.concurrentTasks) {
      return message.nack()
    }

    const task = new app.api.tasks[taskName](app, message)
      // add the current task type into the list of active tasks,
      // so we know who should handle an interrupt call
    taskerClient.activeTasks.get(taskName).push(task)

    app.workerCount++
    Promise.resolve()
      .then(() => {
        return task.run()
      })
      .catch(err => {
        app.log.error(`Error in task.run() for task ${taskName}`, err)
        task.reject()
      }).then(() => {
        return task.finalize()
          .then(() => {
            let oldWorkerCount = app.workerCount
            app.workerCount--
            exports.clearHandler(taskerClient.activeTasks.get(taskName), task)
          })
          .catch(() => {
            let oldWorkerCount = app.workerCount
            app.workerCount--
            exports.clearHandler(taskerClient.activeTasks.get(taskName), task)
          })
      })
  })
}
