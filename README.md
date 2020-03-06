# SQS-UTIL

Custom util to recieve and send message to and from sqs/sns.
Main purpose of the library is to handle messages with size greater than 256KB.

```
Please note: Messages need to be deleted manualy on success.
```

### Installation
```
 npm install https://github.com/LimeTray/node-sqs-helper#tag
```
### USAGE

```
'use strict'
const sqsHelper = require('./index')
const path = require('path')
require('dotenv').config({ path: path.join(__dirname, './.env') })

const awsAccessKeyId = process.env.aws_access_key_id
const awsSecretAccessKey = process.env.aws_secret_access_key
const region = process.env.aws_region
const account = process.env.aws_account

const queueUrl = 'https://sqs.region.amazonaws.com/accountId/queue-name'

// To initiate sqs with credentials
// account is only required if using `getQueueURL` function to genereate url

sqsHelper.initSQS(awsAccessKeyId, awsSecretAccessKey, region, account)

// async message handler

/**
 * Message needs to be deleted manually on success using `sqsHelper.deleteMessage`
 * @param {*} message single message from sqs
 */
const asyncfunction = function (message) {
  return new Promise(function (resolve) {
    console.log(message.ReceiptHandle)
    return resolve(message.ReceiptHandle)
  }).then(function (handle) {
    sqsHelper.deleteMessage({
      QueueUrl: queueUrl,
      ReceiptHandle: handle
    })
  })
}

// Strat consuming messages for queueURL with number of message count (MAX 10)
sqsHelper.startConsumer({
  QueueUrl: queueUrl,
  MaxNumberOfMessages: 5
}, asyncfunction)
```


