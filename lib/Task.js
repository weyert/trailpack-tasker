'use strict'
const _ = require('lodash');

module.exports = class Task {
  constructor (app, message) {
    this.app = app
    this.message = message
    this.id = message.body.taskId
    this.isAcknowledged = false
  }

  ack() {
    if (!this.isAcknowledged) {
      this.isAcknowledged = true
      this.message.ack()
    }
    else {
      this.app.log.warn('Attempting to ack a message that already responded')
    }
  }

  nack() {
    if (!this.isAcknowledged) {
      this.isAcknowledged = true
      this.message.nack()
    }
    else {
      this.app.log.warn('Attempting to nack a message that already responded')
    }
  }

  reject() {
    if (!this.isAcknowledged) {
      this.isAcknowledged = true
      this.message.reject()
    }
    else {
      this.app.log.warn('Attempting to reject a message that already responded')
    }
  }

  run () {
    throw new Error('Subclasses must override Task.run')
  }

  run (msg) {
    this.app.log.debug('Task Retry:', msg)
  }


  interrupt (msg) {
    this.app.log.debug('Task Interrupt:', msg)
  }

  retry (msg) {
    this.app.log.debug('Task Retry:', msg)
  }

  finalize (results) {
    this.app.log.debug('Task Finalize:', results)
  }

  /**
   * Requeue the message so that we can retry to process the task at a later time
   * exponential backoff will be used to calculate the delay for the next time a similar
   * message will be delivered to the main work queue
   */
  requeueTaskForRetry(taskName) {
    return new Promise((resolve, reject) => {
      const taskHeaders = this.message.properties.headers || {};
      const deadHeaders = taskHeaders['x-death'] || [];
      const firstDeadHeader = deadHeaders[0] || {};
      const retryCount = parseInt(taskHeaders['x-retry-count']) || 0;
      const previousMessageDelay = parseInt(taskHeaders['x-delay']) || 0;
      const expiration = parseInt(firstDeadHeader['original-expiration']) || 0;

      const maxRetries = this.app.tasker.maxRetries;
      const retryDelay = this.app.tasker.retryDelay;

      const shouldRetryMessage = retryCount <= maxRetries - 1;

      if (shouldRetryMessage) {
        const messageDelay = Math.pow(4, retryCount) * retryDelay;
        const messagePayload = {
          ..._.omit(this.message.body, ['taskId']),
        };

        // Send that we have received the message and it was all "good"
        this.ack();

        // Dispatch a new job on the `retry`-queue so we can reschedule a delay message publication
        const delayInMinutes = (messageDelay/1000) / 60;
        this.app.log.debug(`Retrying the failed task, retry count: ${retryCount}/${maxRetries}, retrying? ${shouldRetryMessage}, waitingTimeBeforeQueueing: ${messageDelay} (${delayInMinutes} mins) previousMessageDelay: ${previousMessageDelay} originalExpiration: ${expiration}`);
        const sanitisedHeaders = _.omit(taskHeaders, ['x-death', 'x-retry-count', 'x-delay']);
        this.app.tasker.retryPublish(`${taskName}.retry`,
          messagePayload,
          {
            ...sanitisedHeaders,
            'x-retry-count': retryCount + 1,
            'x-retry': _.omit(taskHeaders, ['x-death']) || {},
            'x-delay': messageDelay,
          },
          0
        );

        resolve()
      } else {
        this.app.log.debug(`The total of retries (${retryCount}/${maxRetries}) for this task have been executed. Rejecting the message finally.`);
        this.reject();
        resolve()
      }
    })
  }
}
