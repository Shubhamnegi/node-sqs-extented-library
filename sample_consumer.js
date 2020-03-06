'use strict'
const sqsHelper = require('./index')
const path = require('path')
require('dotenv').config({ path: path.join(__dirname, './.env') })

const awsAccessKeyId = process.env.aws_access_key_id
const awsSecretAccessKey = process.env.aws_secret_access_key
const region = process.env.aws_region
const account = process.env.aws_account

const queueUrl = 'test-queue-url'

sqsHelper.initSQS(awsAccessKeyId, awsSecretAccessKey, region, account)

/**
 *
 * @param {*} message
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

sqsHelper.startConsumer({
  QueueUrl: queueUrl,
  MaxNumberOfMessages: 5
}, asyncfunction)
