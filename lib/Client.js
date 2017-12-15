'use strict'

const uuid = require('uuid')

module.exports = class Client  {

  constructor (app, rabbit, exchangeName, retryExchangeName, maxRetries, retryDelay) {
    this.app = app
    this.rabbit = rabbit
    this.exchangeName = exchangeName
    this.retryExchangeName = retryExchangeName
    this.maxRetries = maxRetries
    this.retryDelay = retryDelay
    this.activeTasks = new Map()
  }

  publish (routingKey, data, headers, expiration) {
    const taskId = uuid.v1()
    data.taskId = taskId

    // Generate the payload to be send to the messaging queue
    const timestamp = Date.now();
    const messageExpiration = expiration || 1000;
    const messagePayload = {
      appId: undefined,
      type: routingKey,
      body: data,
      routingKey: undefined,
      correlationId: undefined,
      sequenceNo: undefined,
      timestamp: timestamp,
      messageId: taskId,
      expiresAfter: messageExpiration,
      headers: headers || {},
      connectionName: "default",
    }

    return this.rabbit.publish(this.exchangeName, messagePayload)
      .then(() => {
        return taskId
      })
  }

  retryPublish (routingKey, data, headers, expiration) {
    const newTaskId = uuid.v1()
    data.taskId = newTaskId

    // Generate the payload to be send to the messaging queue
    const timestamp = Date.now();
    const messageExpiration = expiration || 1000;

    const messagePayload = {
      appId: undefined,
      type: routingKey,
      body: data,
      routingKey: undefined,
      correlationId: undefined,
      sequenceNo: undefined,
      messageId: newTaskId,
      timestamp: timestamp,
      expiresAfter: messageExpiration,
      headers: headers || {},
      connectionName: "default",
    }

    this.app.log.info('Retrying task %s (%s) with delay %sms to exchange: %s', routingKey, newTaskId, messageExpiration, this.retryExchangeName)
    return this.rabbit.publish(this.retryExchangeName, messagePayload)
      .then(() => {
        return newTaskId
      })
  }

  cancelTask (taskName, taskId) {
    return this.rabbit.publish(this.exchangeName, `${taskName}.interrupt`, {
      taskId
    })
  }
}
