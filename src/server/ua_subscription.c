#include "ua_subscription.h"
#include "ua_server_internal.h"
#include "ua_services.h"
#include "ua_nodestore.h"

/*****************/
/* MonitoredItem */
/*****************/

UA_MonitoredItem * UA_MonitoredItem_new() {
    UA_MonitoredItem *new = UA_malloc(sizeof(UA_MonitoredItem));
    new->currentQueueSize = 0;
    new->maxQueueSize = 0;
    // FIXME: This is currently hardcoded;
    new->monitoredItemType = MONITOREDITEM_TYPE_CHANGENOTIFY;
    TAILQ_INIT(&new->queue);
    UA_NodeId_init(&new->monitoredNodeId);
    new->lastSampledValue = UA_BYTESTRING_NULL;
    memset(&new->sampleJobGuid, 0, sizeof(UA_Guid));
    new->sampleJobIsRegistered = false;
    return new;
}

void MonitoredItem_delete(UA_Server *server, UA_MonitoredItem *monitoredItem) {
    MonitoredItem_unregisterSampleJob(server, monitoredItem);
    /* clear the queued samples */
    MonitoredItem_queuedValue *val, *val_tmp;
    TAILQ_FOREACH_SAFE(val, &monitoredItem->queue, listEntry, val_tmp) {
        TAILQ_REMOVE(&monitoredItem->queue, val, listEntry);
        UA_DataValue_deleteMembers(&val->value);
        UA_free(val);
    }
    monitoredItem->currentQueueSize = 0;
    LIST_REMOVE(monitoredItem, listEntry);
    UA_ByteString_deleteMembers(&monitoredItem->lastSampledValue);
    UA_NodeId_deleteMembers(&monitoredItem->monitoredNodeId);
    UA_free(monitoredItem);
}

static void SampleCallback(UA_Server *server, UA_MonitoredItem *monitoredItem) {
    // TODO: Only data changes are currently supported
    if(monitoredItem->monitoredItemType != MONITOREDITEM_TYPE_CHANGENOTIFY)
        return;

    MonitoredItem_queuedValue *newvalue = UA_malloc(sizeof(MonitoredItem_queuedValue));
    if(!newvalue)
        return;
    UA_DataValue_init(&newvalue->value);
    newvalue->clientHandle = monitoredItem->clientHandle;
  
    /* Read the value */
    UA_ReadValueId rvid;
    UA_ReadValueId_init(&rvid);
    rvid.nodeId = monitoredItem->monitoredNodeId;
    rvid.attributeId = monitoredItem->attributeID;
    // todo: use the session attached to the subscription
    Service_Read_single(server, &adminSession, monitoredItem->timestampsToReturn, &rvid, &newvalue->value);

    /* encode to see if the data has changed */
    size_t binsize = UA_calcSizeBinary(&newvalue->value, &UA_TYPES[UA_TYPES_DATAVALUE]);
    UA_ByteString newValueAsByteString;
    UA_StatusCode retval = UA_ByteString_allocBuffer(&newValueAsByteString, binsize);
    if(retval != UA_STATUSCODE_GOOD) {
        UA_DataValue_deleteMembers(&newvalue->value);
        UA_free(newvalue);
        return;
    }
    size_t encodingOffset = 0;
    retval = UA_encodeBinary(&newvalue->value, &UA_TYPES[UA_TYPES_DATAVALUE], &newValueAsByteString, &encodingOffset);
    /* error or the content has not changed */
    if(retval != UA_STATUSCODE_GOOD ||
       (monitoredItem->lastSampledValue.data &&
        UA_String_equal(&newValueAsByteString, &monitoredItem->lastSampledValue))) {
        UA_ByteString_deleteMembers(&newValueAsByteString);
        UA_DataValue_deleteMembers(&newvalue->value);
        UA_free(newvalue);
        return;
    }
  
    /* do we have space? */
    if(monitoredItem->currentQueueSize >= monitoredItem->maxQueueSize) {
        if(!monitoredItem->discardOldest) {
            // We cannot remove the oldest value and theres no queue space left. We're done here.
            UA_ByteString_deleteMembers(&newValueAsByteString);
            UA_DataValue_deleteMembers(&newvalue->value);
            UA_free(newvalue);
            return;
        }
        MonitoredItem_queuedValue *queueItem = TAILQ_LAST(&monitoredItem->queue, QueueOfQueueDataValues);
        TAILQ_REMOVE(&monitoredItem->queue, queueItem, listEntry);
        UA_DataValue_deleteMembers(&queueItem->value);
        UA_free(queueItem);
        monitoredItem->currentQueueSize--;
    }
  
    /* add the sample */
    UA_ByteString_deleteMembers(&monitoredItem->lastSampledValue);
    monitoredItem->lastSampledValue = newValueAsByteString;
    TAILQ_INSERT_TAIL(&monitoredItem->queue, newvalue, listEntry);
    monitoredItem->currentQueueSize++;
}

UA_StatusCode MonitoredItem_registerSampleJob(UA_Server *server, UA_MonitoredItem *mon) {
    UA_Job job = {.type = UA_JOBTYPE_METHODCALL,
                  .job.methodCall = {.method = (UA_ServerCallback)SampleCallback, .data = mon} };
    UA_StatusCode retval = UA_Server_addRepeatedJob(server, job, (UA_UInt32)mon->samplingInterval,
                                                    &mon->sampleJobGuid);
    if(retval == UA_STATUSCODE_GOOD)
        mon->sampleJobIsRegistered = true;
    return retval;
}

UA_StatusCode MonitoredItem_unregisterSampleJob(UA_Server *server, UA_MonitoredItem *mon) {
    if(!mon->sampleJobIsRegistered)
        return UA_STATUSCODE_GOOD;
    mon->sampleJobIsRegistered = false;
    return UA_Server_removeRepeatedJob(server, mon->sampleJobGuid);
}

/****************/
/* Subscription */
/****************/

UA_Subscription *UA_Subscription_new(UA_Session *session, UA_UInt32 subscriptionID) {
    UA_Subscription *new = UA_malloc(sizeof(UA_Subscription));
    if(!new)
        return NULL;
    new->session = session;
    new->subscriptionID = subscriptionID;
    new->sequenceNumber = 1;
    new->currentKeepAliveCount = 0;
    new->maxKeepAliveCount = 0;
    memset(&new->publishJobGuid, 0, sizeof(UA_Guid));
    new->publishJobIsRegistered = false;
    LIST_INIT(&new->retransmissionQueue);
    LIST_INIT(&new->MonitoredItems);
    return new;
}

void UA_Subscription_deleteMembers(UA_Subscription *subscription, UA_Server *server) {
    Subscription_unregisterPublishJob(server, subscription);

    /* Delete monitored Items */
    UA_MonitoredItem *mon, *tmp_mon;
    LIST_FOREACH_SAFE(mon, &subscription->MonitoredItems, listEntry, tmp_mon) {
        LIST_REMOVE(mon, listEntry);
        MonitoredItem_delete(server, mon);
    }

    /* Delete Retransmission Queue */
    UA_NotificationMessageEntry *nme, *nme_tmp;
    LIST_FOREACH_SAFE(nme, &subscription->retransmissionQueue, listEntry, nme_tmp) {
        LIST_REMOVE(nme, listEntry);
        UA_NotificationMessage_deleteMembers(&nme->message);
        UA_free(nme);
    }
}

UA_MonitoredItem *
UA_Subscription_getMonitoredItem(UA_Subscription *sub, UA_UInt32 monitoredItemID) {
    UA_MonitoredItem *mon;
    LIST_FOREACH(mon, &sub->MonitoredItems, listEntry) {
        if(mon->itemId == monitoredItemID)
            break;
    }
    return mon;
}

UA_StatusCode
UA_Subscription_deleteMonitoredItem(UA_Server *server, UA_Subscription *sub, UA_UInt32 monitoredItemID) {
    UA_MonitoredItem *mon;
    LIST_FOREACH(mon, &sub->MonitoredItems, listEntry) {
        if(mon->itemId == monitoredItemID) {
            LIST_REMOVE(mon, listEntry);
            MonitoredItem_delete(server, mon);
            return UA_STATUSCODE_GOOD;
        }
    }
    return UA_STATUSCODE_BADMONITOREDITEMIDINVALID;
}

static void PublishCallback(UA_Server *server, UA_Subscription *sub) {
    /* See if any new data is available */
    UA_Boolean have_response = true;
    size_t notifications = 0;
    MonitoredItem_queuedValue *value = UA_alloca(sub->notificationsPerPublish * sizeof(MonitoredItem_queuedValue));
    UA_MonitoredItem *mon;
    LIST_FOREACH(mon, &sub->MonitoredItems, listEntry) {
        MonitoredItem_queuedValue *qv, *qv_tmp;
        TAILQ_FOREACH_SAFE(qv, &mon->queue, listEntry, qv_tmp) {
            if(notifications >= sub->notificationsPerPublish)
                break;
            value[notifications] = *qv;
            TAILQ_REMOVE(&mon->queue, qv, listEntry);
            UA_free(qv);
            mon->currentQueueSize--;
            notifications++;
            have_response = true;
        }
    }

    /* Continue only if we have data or want to send a keepalive */
    if(!have_response) {
        sub->currentKeepAliveCount++;
        if(sub->currentKeepAliveCount < sub->maxKeepAliveCount) {
            return;
        }
    }
    sub->currentKeepAliveCount = 0;

    /* Dequeue a response */
    UA_PublishResponseEntry *pre = SIMPLEQ_FIRST(&sub->session->responseQueue);
    if(!pre) {
        return; /* there is nothing we can do here */
    }
    SIMPLEQ_REMOVE_HEAD(&sub->session->responseQueue, listEntry);

    /* fill up the response */
    UA_PublishResponse *response = &pre->response;
    response->responseHeader.timestamp = UA_DateTime_now();
    response->subscriptionId = sub->subscriptionID;
    UA_NotificationMessage *message = &response->notificationMessage;
    message->sequenceNumber = ++(sub->sequenceNumber);
    message->publishTime = response->responseHeader.timestamp;
    message->notificationData = UA_ExtensionObject_new();
    message->notificationDataSize = 1;
    UA_ExtensionObject *data = message->notificationData;
    UA_DataChangeNotification *dcn = UA_DataChangeNotification_new();
    dcn->monitoredItems = UA_Array_new(notifications, &UA_TYPES[UA_TYPES_MONITOREDITEMNOTIFICATION]);
    dcn->monitoredItemsSize = notifications;
    for(size_t i = 0; i < notifications; i++) {
        UA_MonitoredItemNotification *min = &dcn->monitoredItems[i];
        min->clientHandle = value[i].clientHandle;
        min->value = value[i].value;
    }
    data->encoding = UA_EXTENSIONOBJECT_DECODED;
    data->content.decoded.data = dcn;
    data->content.decoded.type = &UA_TYPES[UA_TYPES_DATACHANGENOTIFICATION];

    /* Get the available sequence numbers from the retransmission queue */
    size_t available = 0;
    UA_NotificationMessageEntry *nme;
    LIST_FOREACH(nme, &sub->retransmissionQueue, listEntry) {
        available++;
    }
    response->availableSequenceNumbers = UA_malloc(available * sizeof(UA_UInt32));
    response->availableSequenceNumbersSize = available;
    size_t i = 0;
    LIST_FOREACH(nme, &sub->retransmissionQueue, listEntry) {
        response->availableSequenceNumbers[i] = nme->message.sequenceNumber;
        i++;
    }
    
    /* send out the response */
    UA_SecureChannel *channel = sub->session->channel;
    if(channel)
        UA_SecureChannel_sendBinaryMessage(channel, pre->requestId, response,
                                           &UA_TYPES[UA_TYPES_PUBLISHRESPONSE]);

    /* put the notification message into the retransmission queue and delete the response */
    UA_NotificationMessageEntry *retransmission = malloc(sizeof(UA_NotificationMessageEntry));
    retransmission->message = response->notificationMessage;
    UA_NotificationMessage_init(&response->notificationMessage);
    LIST_INSERT_HEAD(&sub->retransmissionQueue, retransmission, listEntry);

    /* remove the response */
    UA_PublishResponse_deleteMembers(response);
    UA_free(pre);
}

UA_StatusCode Subscription_registerPublishJob(UA_Server *server, UA_Subscription *sub) {
    UA_Job job = (UA_Job) {.type = UA_JOBTYPE_METHODCALL,
                           .job.methodCall = {.method = (UA_ServerCallback)PublishCallback, .data = sub} };
    UA_StatusCode retval = UA_Server_addRepeatedJob(server, job,
                                                    (UA_UInt32)sub->publishingInterval,
                                                    &sub->publishJobGuid);
    if(retval == UA_STATUSCODE_GOOD)
        sub->publishJobIsRegistered = true;
    return retval;
}

UA_StatusCode Subscription_unregisterPublishJob(UA_Server *server, UA_Subscription *sub) {
    if(!sub->publishJobIsRegistered)
        return UA_STATUSCODE_GOOD;
    sub->publishJobIsRegistered = false;
    return UA_Server_removeRepeatedJob(server, sub->publishJobGuid);
}
