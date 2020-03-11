'use strict'
const AWS = require('aws-sdk')
const debug = require('debug')('sqshelper')
const uuid = require('uuidv4').uuid

let ACCOUNT_ID = null
/**
 * @type {AWS.SQS}
 */
let SQS = null

/**
 * @type {AWS.S3}
 */
let S3 = null

const ACTION_TYPE = {
  CREATE: 'CREATE',
  UPDATE: 'UPDATE',
  DELETE: 'DELETE',
  PATCH: 'PATCH',
  DEFAULT: 'DEFAULT'
}

const DEFAULT_MESSAGE_SIZE_THRESHOLD = 256 * 1000
// const DEFAULT_MESSAGE_SIZE_THRESHOLD = 10
const RESERVED_ATTRIBUTE_NAME = 'SQSLargePayloadSize'
const S3_BUCKET_NAME_MARKER = '-..s3BucketName..-'
const S3_KEY_MARKER = '-..s3Key..-'
const DISABLE_DELETE_FROM_S3 = Boolean(process.env.DISABLE_DELETE_FROM_S3) || true

/**
 * To create sqs session
 * @param {*} awsId
 * @param {*} awsSecret
 * @param {*} region
 * @param {*} accID
 */
function initSQS (awsId, awsSecret, region, accID) {
  // Update aws credentials

  debug('authenticating using id:' + awsId + ' , secret:' + awsSecret + ', region:' + region + ', account:' + accID)

  SQS = new AWS.SQS({
    apiVersion: '2012-11-05',
    accessKeyId: awsId,
    secretAccessKey: awsSecret,
    region: region
  })

  S3 = new AWS.S3({
    // apiVersion: '2012-11-05',
    accessKeyId: awsId,
    secretAccessKey: awsSecret,
    region: region
  })
  ACCOUNT_ID = accID
}

const ENABLE_LONG_POLLING = true

/**
 * To get absolute queue url using region and accountId
 * @param {string} queueName Name of the queue
 * @returns {Promise<string>}
 */
function getQueueUrl (queueName) {
  return new Promise(function (resolve, reject) {
    if (!ACCOUNT_ID) {
      return reject(new Error('missing account id'))
    }
    if (!queueName || queueName === '') {
      return reject(new Error('invalid queue name'))
    }
    const params = {
      QueueName: queueName,
      QueueOwnerAWSAccountId: ACCOUNT_ID
    }
    SQS.getQueueUrl(params, function (err, data) {
      if (err) return reject(err)
      debug('queueURL:' + data.QueueUrl)
      return resolve(data.QueueUrl)
    })
  })
}

/**
 * Size of string
 * @param {string} message
 * @returns {string}
 */
function getStringSizeInBytes (message) {
  return Buffer.from(message, 'utf-8').length + ''
}

/**
 * To create message object
 * @param {*} messageBody
 * @param {*} actionType
 * @param {*} requestId
 * @returns {{payload:string,action:string,requestId:string}}
 */
function formatSQSMessage (messageBody, actionType, requestId) {
  if (typeof messageBody === 'undefined' || messageBody === '') {
    debug('invalid message, message cant be blank or null')
    throw new Error('invalid message, message cant be blank or null')
  }
  if (!actionType) {
    debug('missing action type')
    throw new Error('missing action type')
  }
  if (ACTION_TYPE[actionType] == null) {
    debug('invalid action type')
    throw new Error('invalid action type')
  }
  const body = {
    payload: messageBody,
    action: actionType
  }
  if (requestId) {
    // append request id if exist
    body.requestId = requestId
  }
  return JSON.stringify(body)
}

/**
 * @param {*} message
 * @returns {Promise<{s3BucketName: string, s3Key: string}>}
 */
function uploadToS3 (message) {
  return new Promise(function (resolve, reject) {
    if (!message) {
      debug('invalid message')
      return reject(new Error('invalid message'))
    }
    const SQS_LARGE_PAYLOAD_S3_BUCKET_NAME = process.env.SQS_LARGE_PAYLOAD_S3_BUCKET_NAME
    if (!SQS_LARGE_PAYLOAD_S3_BUCKET_NAME) {
      debug('invalid bucket name')
      return reject(new Error('invalid bucket name'))
    }
    const id = uuid()
    debug('message will be uploaded with id:' + id)

    S3.putObject({
      Bucket: SQS_LARGE_PAYLOAD_S3_BUCKET_NAME,
      Key: id,
      Body: message,
      ContentType: 'text/plain'
    }, function (err, data) {
      if (err) {
        return reject(err)
      }
      debug('upload to s3 response: ' + JSON.stringify(data))
      return resolve({
        s3BucketName: SQS_LARGE_PAYLOAD_S3_BUCKET_NAME,
        s3Key: id
      })
    })
  })
}

/**
 * To get total size for message attributes
 * @param {AWS.SQS.MessageBodyAttributeMap} messageAttributes
 */
const getSizeOfMessageAttributes = (messageAttributes) => {
  let totalSize = 0
  if (messageAttributes) {
    Object.keys(messageAttributes).forEach(key => {
      totalSize += getStringSizeInBytes(key)
      const value = messageAttributes[key]
      if (value.DataType) {
        totalSize += getStringSizeInBytes(value.DataType)
      }
      if (value.StringValue) {
        totalSize += getStringSizeInBytes(value.StringValue)
      }
      if (value.BinaryValue) {
        totalSize += getStringSizeInBytes(value.BinaryValue)
      }
    })
  }
  return totalSize
}

/**
 * To check of total size of message is greater than threshold
 * @param {string} message
 * @param {AWS.SQS.MessageBodyAttributeMap} messageAttributes
 */
const isLarge = (message, messageAttributes) => {
  const messageSize = getStringSizeInBytes(message)
  const messageAttributesSize = getSizeOfMessageAttributes(messageAttributes)
  return (messageSize + messageAttributesSize > DEFAULT_MESSAGE_SIZE_THRESHOLD)
}

/**
 *
 * @param {AWS.SQS.SendMessageRequest} param
 * @returns {Promise<AWS.SQS.SendMessageRequest>}
 */
function prepareMessage (param) {
  return new Promise(function (resolve, reject) {
    if (!isLarge(param.MessageBody, param.MessageAttributes)) {
      return resolve(param)
    } else {
      // Upload to s3 and get s3 link
      return uploadToS3(param.MessageBody)
        .then(function (obj) {
          // convert to string
          param.MessageAttributes[RESERVED_ATTRIBUTE_NAME] = { DataType: 'Number', StringValue: getStringSizeInBytes(param.MessageBody) }
          param.MessageBody = JSON.stringify(obj)
          return resolve(param)
        }).catch(function (error) {
          return reject(error)
        })
    }
  })
}

/**
 * @param {AWS.SQS.SendMessageRequest} param
 * @returns {Promise<AWS.SQS.SendMessageResult>} messageId
 */
function sendMessage (param) {
  if (!param.MessageAttributes) {
    param.MessageAttributes = {}
  }
  return prepareMessage(param)
    .then(function (resp) {
      return SQS.sendMessage(resp).promise()
    })
}

/**
 *
 * @param {*} bucket
 * @param {*} key
 */
function getTextFromS3 (bucket, key) {
  debug('fetchine message from s3:' + bucket + ',with key:' + key)
  return S3.getObject({ Bucket: bucket, Key: key }).promise()
}

/**
 *
 * @param {string} receiptHandle
 * @param {string} s3MsgBucketName
 * @param {string} s3MsgKey
 * @returns {string}
 */
function embedS3PointerInReceiptHandle (receiptHandle, s3MsgBucketName, s3MsgKey) {
  const modifiedReceiptHandle = S3_BUCKET_NAME_MARKER + s3MsgBucketName + S3_BUCKET_NAME_MARKER + S3_KEY_MARKER + s3MsgKey + S3_KEY_MARKER + receiptHandle
  return modifiedReceiptHandle
}

/**
 *
 * @param {AWS.SQS.Message} message
 */
function fixReceivedMessage (message) {
  if (message.MessageAttributes && message.MessageAttributes[RESERVED_ATTRIBUTE_NAME] != null) {
    /**
    * @type {{s3BucketName: string, s3Key: string}}
    */
    const messageBody = JSON.parse(message.Body)
    const s3MsgBucketName = messageBody.s3BucketName
    const s3MsgKey = messageBody.s3Key
    return getTextFromS3(s3MsgBucketName, s3MsgKey)
      .then(function (data) {
        const originalMessageBody = data.Body
        debug('s3 body: ' + originalMessageBody)
        message.Body = originalMessageBody.toString()
        delete message.MessageAttributes[RESERVED_ATTRIBUTE_NAME]
        // update s3 reciept Handle
        const modifiedReceiptHandle = embedS3PointerInReceiptHandle(message.ReceiptHandle, s3MsgBucketName, s3MsgKey)
        message.ReceiptHandle = modifiedReceiptHandle
        return message
      })
  } else {
    return Promise.resolve(message)
  }
}

/**
 * @param {AWS.SQS.ReceiveMessageRequest} param
 * @returns {Promise<Array<AWS.SQS.Message>>}
 */
function receiveMessage (param) {
  debug('requested receiveMessage for ' + param.QueueUrl)
  const messageAttributeNames = param.MessageAttributeNames
  if (messageAttributeNames && messageAttributeNames > 0) {
    const index = messageAttributeNames.findIndex(function (x) { return x === RESERVED_ATTRIBUTE_NAME })
    if (index < 0) {
      // RESERVED_ATTRIBUTE_NAME is missing, hence append
      messageAttributeNames.push(RESERVED_ATTRIBUTE_NAME)
    }
  } else {
    param.MessageAttributeNames = [RESERVED_ATTRIBUTE_NAME]
  }

  if (param.WaitTimeSeconds == null && ENABLE_LONG_POLLING) {
    param.WaitTimeSeconds = 20
  }

  debug('receiveMessage request:' + JSON.stringify(param))
  return SQS.receiveMessage(param).promise()
    .then(function (result) {
      if (result.Messages && result.Messages.length) {
        return Promise.all(result.Messages.map(function (message) {
          return fixReceivedMessage(message)
        }))
      } else {
        return Promise.resolve([]);
      }
    })
}

/**
 *
 * @param {string} receiptHandle
 * @param {string} marker
 */
function getFromReceiptHandleByMarker (receiptHandle, marker) {
  const firstOccurence = receiptHandle.indexOf(marker)
  const secondOccurence = receiptHandle.indexOf(marker, firstOccurence + 1)
  return receiptHandle.substring(firstOccurence + marker.length, secondOccurence)
}

/**
 *
 * @param {string} receiptHandle
 */
function getOrigReceiptHandle (receiptHandle) {
  const secondOccurence = receiptHandle.indexOf(S3_KEY_MARKER,
    receiptHandle.indexOf(S3_KEY_MARKER) + 1)
  return receiptHandle.substring(secondOccurence + S3_KEY_MARKER.length)
}

/**
 *
 * @param {AWS.SQS.DeleteMessageRequest} deleteRequest
 */
function deleteMessageFromS3 (deleteRequest) {
  const receiptHandle = deleteRequest.ReceiptHandle
  const s3MsgBucketName = getFromReceiptHandleByMarker(receiptHandle, S3_BUCKET_NAME_MARKER)
  const s3MsgKey = getFromReceiptHandleByMarker(receiptHandle, S3_KEY_MARKER)
  const origReceiptHandle = getOrigReceiptHandle(receiptHandle)
  deleteRequest.ReceiptHandle = origReceiptHandle
  debug('original ReceiptHandle:' + origReceiptHandle)
  if (DISABLE_DELETE_FROM_S3) {
    debug('igonring delelete from s3')
    return Promise.resolve(deleteRequest)
  }
  debug('deleting from bucket ' + s3MsgBucketName + ' with key' + s3MsgKey)
  return S3.deleteObject({
    Bucket: s3MsgBucketName, Key: s3MsgKey
  }).promise()
    .then(function () {
      return deleteRequest
    })
}

/**
 *
 * @param {AWS.SQS.DeleteMessageRequest} deleteRequest
 */
function deleteMessageFromS3Batch (deleteBatchRequest) {
  if (DISABLE_DELETE_FROM_S3) {
    debug('igonring delelete from s3')
    return Promise.resolve(deleteBatchRequest)
  }
  var bucketWiseKeys = {}
  deleteBatchRequest.Entries.forEach(function (entry) {
    if (isS3ReceiptHandle(entry.ReceiptHandle)) {
      const receiptHandle = entry.ReceiptHandle
      const s3MsgBucketName = getFromReceiptHandleByMarker(receiptHandle, S3_BUCKET_NAME_MARKER)
      const s3MsgKey = getFromReceiptHandleByMarker(receiptHandle, S3_KEY_MARKER)
      const origReceiptHandle = getOrigReceiptHandle(receiptHandle)
      entry.ReceiptHandle = origReceiptHandle
      if (!bucketWiseKeys[s3MsgBucketName]) {
        bucketWiseKeys[s3MsgBucketName] = []
      }
      bucketWiseKeys[s3MsgBucketName].push(s3MsgKey)
      debug('original ReceiptHandle:' + origReceiptHandle)
      debug('deleting from bucket ' + s3MsgBucketName + ' with key' + s3MsgKey)
    }
  })
  var promises = Object.keys(bucketWiseKeys).map(function (bucket) {
    var deleteParam = {
      Bucket: bucket,
      Delete: {
        Objects: bucketWiseKeys[bucket].map(function (key) {
          return { Key: key }
        })
      }
    }
    return S3.deleteObjects(deleteParam).promise()
  })

  return Promise.all(promises).then(function () {
    return deleteBatchRequest
  })
}

/**
 *
 * @param {string} receiptHandle
 * @returns {boolean}
 */
function isS3ReceiptHandle (receiptHandle) {
  return receiptHandle.includes(S3_BUCKET_NAME_MARKER) && receiptHandle.includes(S3_KEY_MARKER)
}

/**
 * To delete message from sqs
 * @param {AWS.SQS.DeleteMessageRequest} param
 */
function deleteMessage (param) {
  debug('deleteMessage ReceiptHandle: ' + param.ReceiptHandle)
  if (!isS3ReceiptHandle(param.ReceiptHandle)) {
    // not large payload
    return SQS.deleteMessage(param).promise()
  } else {
    // is large payload
    return deleteMessageFromS3(param)
      .then(function (result) {
        return SQS.deleteMessage(result).promise()
      })
  }
}

/**
 * To delete message from sqs
 * @param {AWS.SQS.DeleteMessageBatchRequest} param
 */
function deleteMessageBatch (param) {
  debug('deleteMessage ReceiptHandle: ' + param.ReceiptHandle)
  var promises = []
  promises.push(deleteMessageFromS3Batch(param))
  promises.push(SQS.deleteMessageBatch(param))
  return Promise.all(promises)
    .then(function (results) {
      return results[1]
    })
}

/**
 * Promise to sleep program
 * @param {*} ms
 */
function sleep (ms) {
  return new Promise(function (resolve) {
    debug('sleeping for ms:' + ms)
    setTimeout(function () {
      return resolve()
    }, ms)
  })
}

/**
 * This is infinite loop to consume all the messages
 * @param {AWS.SQS.ReceiveMessageRequest} param
 * @param {Promise<*>} param
 */
function startConsumer (param, fn) {
  return receiveMessage(param)
    .then(function (data) {
      const messageLength = data.length
      debug('message length:' + messageLength)
      if (messageLength === 0) {
        const RECEIVER_SLEEP_DURATION = process.env.RECEIVER_SLEEP_DURATION || 5000
        return sleep(RECEIVER_SLEEP_DURATION)
      } else {
        return Promise.all(data.map(function (message) {
          debug('executing handler for ' + message.MessageId)
          return fn(message)
        }))
      }
    })
    .then(function () {
      debug('repeating consumer')
      return startConsumer(param, fn)
    })
    .catch(function (error) {
      throw error
    })
}

module.exports = {
  initSQS: initSQS,
  getQueueUrl: getQueueUrl,
  deleteMessage: deleteMessage,
  deleteMessageBatch: deleteMessageBatch,
  receiveMessage: receiveMessage,
  startConsumer: startConsumer,
  sendMessage: sendMessage,
  actionType: ACTION_TYPE,
  formatSQSMessage: formatSQSMessage
}
