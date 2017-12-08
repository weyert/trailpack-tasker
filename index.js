'use strict'

const Trailpack = require('trailpack')
const lib = require('./lib')
const _ = require('lodash')
const rabbit = require('rabbot')
// automatically nack exceptions in handlers
rabbit.nackOnError()
const joi = require('joi')
const config = require('./lib/config')
const Client = require('./lib/Client')
const TaskerUtils = require('./lib/Util.js')

module.exports = class TaskerTrailpack extends Trailpack {

  /**
   * TODO document method
   */
  validate() {
    this.app.log.debug('trailpackTasker.validate()');
    this.app.config.tasker = _.defaultsDeep(this.app.config.tasker, config.defaults)
    return new Promise((resolve, reject) => {
      joi.validate(this.app.config.tasker, config.schema, (err, value) => {
        if (err) {
          this.app.log.debug('trailpackTasker.validate() Error: ', err);
          return reject(new Error('Tasker Configuration: ' + err))
        }

        return resolve(value)
      })
    })
  }

  /**
   * configure rabbitmq exchanges, queues, bindings and handlers
   */
  configure() {
    this.app.log.debug('trailpackTasker.configure()');
    let taskerConfig = this.app.config.tasker
    this.app.log.debug('[1] taskerConfig: ', taskerConfig)
    const profile = getWorkerProfile(taskerConfig)
    taskerConfig = configureExchangesAndQueues(profile, taskerConfig)
    this.app.log.debug('profile: ', profile)

    this.app.tasker = new Client(this.app, rabbit, taskerConfig.exchangeName)
    TaskerUtils.registerTasks(profile, this.app, rabbit)

    this.app.workerCount = 0
    this.app.api.tasks = this.app.api.tasks || {}
  }

  /**
   * Establish connection to the RabbitMQ exchange, listen for tasks.
   */
  initialize() {
    this.app.log.debug('trailpackTasker.initialize()');

    this.app.on('trails:ready', () => {
      return new Promise((resolve, reject) => {
          rabbit.configure(this.app.config.tasker)
          .then((result => {
            this.app.log.debug('Connected to Rabbitmq', result)
            return resolve(true)
          }))
          .catch(err => {
            this.app.log.debug('Failed to connect to the Rabbitmq cluster', err)
            return resolve(false)
          })
      })
    })
  }

  constructor(app) {
    super(app, {
      config: require('./config'),
      api: require('./api'),
      pkg: require('./package')
    })
  }
}

/**
 * Get the profile for the current process
 * The profile contains a list of tasks that this process can work on
 * If there is no profile (ie the current process is not a worker process), this returns undefined
 */
function getWorkerProfile(taskerConfig) {
  const profileName = taskerConfig.worker
  console.log('trailpackTasker.getWorkerProfile() profileName: %s', profileName);

  if (!profileName || !taskerConfig.profiles[profileName]) {
    return { tasks: [] }
  }

  return taskerConfig.profiles[profileName]
}

/**
 * This function mutates the taskerConfig object
 * Declare the exchanges and queues, and bind them appropriately
 * Define the relevant routing keys
 * @returns {object} - taskerConfig
 */
function configureExchangesAndQueues(profile, taskerConfig) {
  const exchangeName = taskerConfig.exchange || 'tasker-work-x'
  const workQueueName = taskerConfig.workQueueName || 'tasker-work-q'
  const interruptQueueName = taskerConfig.interruptQueueName || 'tasker-interrupt-q'
  const limit = taskerConfig.concurrentTasks || 10
  console.log('trailpackTasker.configureExchangesAndQueues()', exchangeName, workQueueName, interruptQueueName, limit);

  taskerConfig.exchangeName = exchangeName
  taskerConfig.exchanges = [{
    name: exchangeName,
    type: 'topic',
    autoDelete: false
  }]

  taskerConfig.queues = [{
    name: workQueueName,
    autoDelete: false,
    subscribe: true,
    limit: limit,
  }, {
    name: interruptQueueName,
    autoDelete: false,
    subscribe: true,
    limit: limit,
  }]

  taskerConfig.bindings = [{
    exchange: exchangeName,
    target: workQueueName,
    keys: profile.tasks
  }, {
    exchange: exchangeName,
    target: interruptQueueName,
    keys: profile.tasks.map(task => task + '.interrupt')
  }]

  return taskerConfig
}





module.exports.Task = lib.Task
