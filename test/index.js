'use strict'

const assert = require('assert')
const sqsHelper = require('../index')
const path = require('path')
require('dotenv').config({ path: path.join(__dirname, '../.env') })

const awsAccessKeyId = process.env.aws_access_key_id
const awsSecretAccessKey = process.env.aws_secret_access_key
const region = process.env.aws_region
const account = process.env.aws_account

let queueUrl = 'test-queue-url'
let message = null

describe('SQS Helpers', function () {
  before(function (done) {
    sqsHelper.initSQS(awsAccessKeyId, awsSecretAccessKey, region, account)
    done()
  })

  // it('should update text to s3', function (done) {
  //   sqsHelper.uploadToS3('shubham negi')
  //     .then(data => {
  //       console.log(data, 'data')
  //       assert.equal(true, data != null)
  //       assert.equal(true, data.s3BucketName != null)
  //       assert.equal(true, data.s3Key != null)
  //       done()
  //     }).catch(err => {
  //       throw err
  //     })
  // })

  it('should be able to get sqs-url', function (done) {
    sqsHelper.getQueueUrl('test-queue-name')
      .then(function (url) {
        console.log(url, 'url')
	assert.equal(url, queueUrl)
        done()
      }).catch(function (error) {
        throw error
      })
  })

  it('should be able to send message to sqs', function (done) {
    const body = sqsHelper.formatSQSMessage('message text', sqsHelper.actionType.CREATE, 'requestId')
    sqsHelper.sendMessage({
      MessageBody: body,
      QueueUrl: queueUrl
    })
      .then(function (data) {
        assert.equal(true, data != null)
        assert.notEqual(data, '')
        done()
      }).catch(function (error) {
        throw error
      })
  })

  it('should receive array of messages from sqs', function (done) {
    sqsHelper.receiveMessage({
      MaxNumberOfMessages: 1,
      QueueUrl: queueUrl,
      WaitTimeSeconds: 20,
      VisibilityTimeout: 20
    }).then(function (messages) {
      message = messages[0]
      console.log(message)
      assert.equal(true, messages.length > 0)
      assert.equal(true, message.Body != null)

      // const att = message.MessageAttributes
      // assert.equal(true, att.SQSLargePayloadSize != null)
      // assert.equal(true, att.SQSLargePayloadSize.StringValue != null)
      // assert.equal(true, att.SQSLargePayloadSize.DataType === 'Number')

      // const body = JSON.parse(message.Body)

      // if (Number.parseInt(att.SQSLargePayloadSize.StringValue) > 256 * 1000) {
      //   assert.equal(true, body.s3BucketName != null)
      //   assert.equal(true, body.s3Key != null)
      // } else {
      //   assert.equal(true, body.payload != null)
      //   assert.equal(true, body.action != null)
      //   assert.equal(true, body.requestId != null)
      // }
      done()
    }).catch(function (error) {
      throw error
    })
  })

  it('should delete message from sqs', function (done) {
    assert.equal(true, message !== null)
    sqsHelper.deleteMessage({
      QueueUrl: queueUrl,
      ReceiptHandle: message.ReceiptHandle
    })
      .then(function (status) {
        done()
      }).catch(function (error) {
        throw error
      })
  })
})
