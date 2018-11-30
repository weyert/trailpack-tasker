'use strict'

const _ = require('lodash')

exports.registerTasks = function(profile, app, rabbit) {
  profile.tasks.forEach(taskName => {
    app.tasker.activeTasks.set(taskName, [])
    exports.registerRun(taskName, app, rabbit)
    exports.registerInterrupt(taskName, app, rabbit)
    exports.registerRetry(taskName, app, rabbit)
  })
}

exports.clearHandler = function (activeTasks, task) {
  console.log('clearHandler() task: %s', task.id);
  _.remove(activeTasks, activeTask => {
    return task.id = activeTask.id
  })
}

exports.registerInterrupt = function (taskName, app, rabbit) {
  rabbit.handle(`${taskName}.interrupt`, message => {
    const taskId = message.properties.messageId;
    const activeTasks = app.tasker.activeTasks.get(taskName) || []
    const task = _.find(activeTasks, activeTask => {
      return activeTask.id = taskId
    })

    if (!task) {
      app.log.debug(`Failed to interrupt task, no active handler found for task ${taskName}  and id ${taskId}`)
      return message.reject()
    }

    task.interrupt(message)
  })
}

exports.registerRetry = function (taskName, app, rabbit) {
  app.log.debug('registerRetry() taskName: %s', taskName)
  rabbit.handle(`${taskName}.retry`, message => {
    const taskId = message.properties.messageId;
    app.log.debug('!!! INCOMING MESSAGE !!!');

    const activeTasks = app.tasker.activeTasks.get(taskName) || []
    const task = _.find(activeTasks, activeTask => {
      return activeTask.id = taskId
    })

    // If the task doesn't exist yet, we republish the message so it can be handled
    if (!task) {
      message.reject()

      // After we received the task on the retry queue, we should schedule it for direct delivery!
      const messageReceivedAt = new Date() // message.properties.headers['timestamp_in_ms'] || 0;
      const taskHeaders = _.omit(message.properties.headers, ['x-delay']) || {};
      const messagePayload = message.body

      app.log.debug(`No existing ${taskName}-task with id: ${taskId} messageReceivedAt: ${messageReceivedAt}`)
      app.log.debug('Message Headers: ', taskHeaders)
      app.tasker.publish(taskName, messagePayload, taskHeaders)
      return
    }

    app.log.debug(`Task defined for task name: ${taskName} processing id ${taskId}`)
    task.retry()
  })
}

exports.registerRun = function (taskName, app, rabbit) {
  const taskerClient = app.tasker

  // set up the task handler
  rabbit.handle(taskName, message => {
    app.log.debug('Received `run` message from RabbitMQ');

    if (!app.api.tasks[taskName]) {
      app.log.debug(`No task defined for task name: ${taskName}. Message body was: ${JSON.stringify(message.body)}`)
      return message.reject()
    }

    if (app.workerCount >= app.config.tasker.concurrentTasks) {
      app.log.debug(`Sending 'nack'-message because workerCount >= concurrentTasks ${app.workerCount} >= ${app.config.tasker.concurrentTasks} totalTasks: `, taskerClient.activeTasks)
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
        app.log.debug('Completed!!! registerRun()')
        return task.finalize()
          .then(() => {
            let oldWorkerCount = app.workerCount
            app.workerCount--
            exports.clearHandler(taskerClient.activeTasks.get(taskName), task)
          })
          .catch((err) => {
            app.log.error(`[2] Error in task.run() for task ${taskName}`, err)
            let oldWorkerCount = app.workerCount
            app.workerCount--
            exports.clearHandler(taskerClient.activeTasks.get(taskName), task)
          })
      })
  })
}
