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
    const messageHeaders = {
      ...headers
    }

    const messagePayload = {
      appId: undefined,
      type: routingKey,
      body: data,
      routingKey: routingKey,
      correlationId: undefined,
      messageId: taskId,
      sequenceNo: undefined,
      timestamp: timestamp,
      headers: messageHeaders || {},
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
    const messageDelayTimeout = headers['x-delay'] || 0;
    const messageHeaders = {
      ...headers
    }

    const messagePayload = {
      appId: undefined,
      type: routingKey,
      body: data,
      routingKey: routingKey,
      correlationId: undefined,
      messageId: newTaskId,
      sequenceNo: undefined,
      timestamp: timestamp,
      headers: messageHeaders || {},
      connectionName: "default",
    }

    console.log('Queued a new job for the failed task.', messagePayload.headers);
    return this.rabbit.publish(this.retryExchangeName, messagePayload)
      .then(() => {
        console.log('Published message retry: %s', newTaskId)
        return newTaskId
      })
  }

  cancelTask (taskName, taskId) {
    return this.rabbit.publish(this.exchangeName, `${taskName}.interrupt`, {
      taskId
    })
  }
}
