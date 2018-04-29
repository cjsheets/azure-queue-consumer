# azure-queue-consumer

[![Build Status](https://travis-ci.org/cjsheets/azure-queue-consumer.svg)](https://travis-ci.org/cjsheets/azure-queue-consumer)

Build Azure data queue-based applications without the boilerplate. Just define a function that receives an Azure data queue message and call a callback when the message has been processed.

## Installation

```bash
npm install azure-queue-consumer --save
```

## Usage

```js
const Consumer = require('azure-queue-consumer');

const app = Consumer.create({
  queueUrl: 'https://account-name.queue.core.windows.net/queue-name',
  handleMessage: (message, done) => {
    // do some work with `message`
    done();
  }
});

app.on('error', (err) => {
  console.log(err.message);
});

app.start();
```

* The queue is polled every second by default.
* Messages are deleted from the queue once `done()` is called.
* Messages are processed one at a time.

### Credentials

By default the consumer will look for Azure credentials in the places [specified by the Azure Storage SDK](http://azure.github.io/azure-storage-node/#toc4). The simplest option is to export your credentials as environment variables:

```bash
export AZURE_STORAGE_CONNECTION_STRING=...
```

If you need to specify your credentials manually, you can use a pre-configured instance of the [Azure QueueService](http://azure.github.io/azure-storage-node/#toc7__anchor):


```js
const Consumer = require('azure-queue-consumer');
const azureStorage = require('azure-storage');

const app = Consumer.create({
  queueUrl: 'https://account-name.queue.core.windows.net/queue-name',
  handleMessage: (message, done) => {
    // ...
    done();
  },
  queueService: azureStorage.createQueueServiceWithSas(
    'https://account-name.queue.core.windows.net/queue-name',
    '<SAS Token>'
  );
});

app.on('error', (err) => {
  console.log(err.message);
});

app.start();
```

## API

### `Consumer.create(options)`

Creates a new Azure data queue consumer.

#### Options

* `queueUrl` - _String_ - The Azure data queue URL
* `handleMessage` - _Function_ - A function to be called whenever a message is received. Receives a queue message object as its first argument and a function to call when the message has been handled as its second argument (i.e. `handleMessage(message, done)`).
* `pollDelaySeconds` - _Number_ - The minimum delay (in seconds) between queue polling attempts.
* `waitTimeSeconds` - _Number_ - maximum execution time across all potential retries, for requests made via the Queue service.
* `authenticationErrorTimeout` - _Number_ - The duration (in milliseconds) to wait before retrying after an authentication error (defaults to `10000`).
* `queueService` - _Object_ - An optional [Azure QueueService](http://azure.github.io/azure-storage-node/#toc7__anchor) instance to use if you need to configure the client manually

### `consumer.start()`

Start polling the queue for messages.

### `consumer.stop()`

Stop polling the queue for messages.

### Events

Each consumer is an [`EventEmitter`](http://nodejs.org/api/events.html) and emits the following events:

|Event|Params|Description|
|-----|------|-----------|
|`error`|`err`, `[message]`|Fired when an error occurs interacting with the queue. If the error correlates to a message, that error is included in Params|
|`processing_error`|`err`, `message`|Fired when an error occurs processing the message.|
|`message_received`|`message`|Fired when a message is received.|
|`message_processed`|`message`|Fired when a message is successfully processed and removed from the queue.|
|`response_processed`|None|Fired after one batch of items (up to `batchSize`) has been successfully processed.|
|`stopped`|None|Fired when the consumer finally stops its work.|
|`empty`|None|Fired when the queue is empty (All messages have been consumed).|


### Credits

Inspired by [SQS Consumer](https://github.com/bbc/sqs-consumer)