'use strict';

const EventEmitter = require('events').EventEmitter;
const async = require('async');
const auto = require('auto-bind');
const azureStorage = require('azure-storage');
const debug = require('debug')('azure-queue-consumer');
const requiredOptions = [
  'queueName',
  'handleMessage'
];

class QueueServiceError extends Error {
  constructor() {
    super(Array.from(arguments));
    this.name = this.constructor.name;
  }
}

function validate(options) {
  requiredOptions.forEach((option) => {
    if (!options[option]) {
      throw new Error('Missing queue service consumer option [' + option + '].');
    }
  });

  if (options.batchSize > 10 || options.batchSize < 1) {
    throw new Error('Queue batchSize option must be between 1 and 10.');
  }
}

function isAuthenticationError(err) {
  return (err.statusCode === 403 || err.code === 'CredentialsError');
}

class Consumer extends EventEmitter {
  constructor(options) {
    super();
    validate(options);

    this.queueName = options.queueName;
    this.handleMessage = options.handleMessage;
    this.attributeNames = undefined;
    this.messageAttributeNames = undefined;
    this.stopped = true;
    this.isWaiting = false;
    this.batchSize = options.batchSize || 1;
    this.visibilityTimeout = undefined;
    this.terminateVisibilityTimeout = undefined;
    this.pollDelaySeconds = options.pollDelaySeconds || 1;
    this.maximumExecutionTimeSeconds = options.maximumExecutionTimeSeconds || 10;
    this.authenticationErrorTimeoutSeconds = options.authenticationErrorTimeoutSeconds || 10;

    this.queueService = options.queueService || new azureStorage.createQueueService();

    auto(this);
  }

  static create(options) {
    return new Consumer(options);
  }

  start() {
    if (this.stopped) {
      debug('Starting consumer');
      this.stopped = false;
      this._poll();
    }
  }

  stop() {
    debug('Stopping consumer');
    this.stopped = true;
  }

  _poll() {
    const receiveParams = {
      numOfMessages: this.batchSize,
      maximumExecutionTimeInMs: this.maximumExecutionTimeSeconds * 1000,
      visibilityTimeout: this.visibilityTimeout
    };

    if (!this.stopped) {
      debug('Polling for messages');
      this.queueService.getMessages(this.queueName, receiveParams, this._handleQueueServiceResponse);
    } else {
      this.emit('stopped');
    }
  }

  _handleQueueServiceResponse(err, response) {
    const consumer = this;

    if (err) {
      this.emit('error', new QueueServiceError('Queue service receive message failed: ' + err.message));
    }

    debug('Received queue service response');
    debug(response);

    if (response && response.length > 0) {
      async.each(response, this._processMessage, () => {
        // start polling again once all of the messages have been processed
        consumer.emit('response_processed');
        consumer._poll();
      });
    } else if (err && isAuthenticationError(err)) {
      // there was an authentication error, so wait a bit before repolling
      debug('There was an authentication error. Pausing before retrying.');
      setTimeout(this._poll.bind(this), this.authenticationErrorTimeoutSeconds * 1000);
    } else {
      // there were no messages, so start polling again
      setTimeout(this._poll.bind(this), this.pollDelaySeconds * 1000);
    }
  }

  _processMessage(message, cb) {
    const consumer = this;

    this.emit('message_received', message);
    async.series([
      function handleMessage(done) {
        try {
          consumer.handleMessage(message, done);
        } catch (err) {
          done(new Error('Unexpected message handler failure: ' + err.message));
        }
      },
      function deleteMessage(done) {
        consumer._deleteMessage(message, done);
      }
    ], (err) => {
      if (err) {
        if (err.name === QueueServiceError.name) {
          consumer.emit('error', err, message);
        } else {
          consumer.emit('processing_error', err, message);
        }

      } else {
        consumer.emit('message_processed', message);
      }
      cb();
    });
  }

  _deleteMessage(message, cb) {

    debug('Deleting message %s', message.messageId);
    this.queueService.deleteMessage(this.queueName, message.messageId, message.popReceipt, (err) => {
      if (err) return cb(new QueueServiceError('Queue service delete message failed: ' + err.message));

      cb();
    });
  }
}

module.exports = Consumer;
