#include "ua_util.h"
#include "ua_server_internal.h"
#include "ua_services.h"
#include "ua_statuscodes.h"
#include "ua_nodestore.h"
#include "ua_session.h"
#include "ua_types_generated_encoding_binary.h"

/**
 * Information Model Consistency
 *
 * The following consistency assertions *always* hold:
 *
 * - There are no directed cycles of hierarchical references
 * - All nodes have a hierarchical relation to at least one father node
 * - Variables and Objects contain all mandatory children according to their type
 *
 * The following consistency assertions *eventually* hold:
 *
 * - All references (except those pointing to external servers with an expandednodeid) are two-way
 *   (present in the source and the target node)
 * - The target of all references exists in the information model
 *
 */

// TOOD: Ensure that the consistency guarantuees hold until v0.2

/**************************/
/* Parse Node Definitions */
/**************************/

/* may _init content in attr */
static void copyStandardAttributes(UA_Node *node, UA_NodeAttributes *attr) {
    if(attr->specifiedAttributes & UA_NODEATTRIBUTESMASK_DISPLAYNAME) {
        node->displayName = attr->displayName;
        UA_LocalizedText_init(&attr->displayName);
    }
    if(attr->specifiedAttributes & UA_NODEATTRIBUTESMASK_DESCRIPTION) {
        node->description = attr->description;
        UA_LocalizedText_init(&attr->description);
    }
    if(attr->specifiedAttributes & UA_NODEATTRIBUTESMASK_WRITEMASK)
        node->writeMask = attr->writeMask;
    if(attr->specifiedAttributes & UA_NODEATTRIBUTESMASK_USERWRITEMASK)
        node->userWriteMask = attr->userWriteMask;
}

static UA_StatusCode parseVariableNode(const UA_ExtensionObject *attributes, UA_Node **new_node) {
    if(attributes->typeId.identifier.numeric !=
       UA_TYPES[UA_TYPES_VARIABLEATTRIBUTES].typeId.identifier.numeric + UA_ENCODINGOFFSET_BINARY)
        return UA_STATUSCODE_BADNODEATTRIBUTESINVALID;

    UA_VariableAttributes attr;
    size_t pos = 0;
    // todo return more informative error codes from decodeBinary
    if(UA_VariableAttributes_decodeBinary(&attributes->body, &pos, &attr) != UA_STATUSCODE_GOOD)
        return UA_STATUSCODE_BADNODEATTRIBUTESINVALID;

    UA_VariableNode *vnode = UA_VariableNode_new();
    if(!vnode) {
        UA_VariableAttributes_deleteMembers(&attr);
        return UA_STATUSCODE_BADOUTOFMEMORY;
    }

    copyStandardAttributes((UA_Node*)vnode, (UA_NodeAttributes*)&attr);
    if(attr.specifiedAttributes & UA_NODEATTRIBUTESMASK_ACCESSLEVEL)
        vnode->accessLevel = attr.accessLevel;
    if(attr.specifiedAttributes & UA_NODEATTRIBUTESMASK_USERACCESSLEVEL)
        vnode->userAccessLevel = attr.userAccessLevel;
    if(attr.specifiedAttributes & UA_NODEATTRIBUTESMASK_HISTORIZING)
        vnode->historizing = attr.historizing;
    if(attr.specifiedAttributes & UA_NODEATTRIBUTESMASK_MINIMUMSAMPLINGINTERVAL)
        vnode->minimumSamplingInterval = attr.minimumSamplingInterval;
    if(attr.specifiedAttributes & UA_NODEATTRIBUTESMASK_VALUERANK)
        vnode->valueRank = attr.valueRank;

    // don't use extra dimension spec. This comes from the value.
    /* if(attr.specifiedAttributes & UA_NODEATTRIBUTESMASK_ARRAYDIMENSIONS) { */
    /*     vnode->arrayDimensionsSize = attr.arrayDimensionsSize; */
    /*     vnode->arrayDimensions = attr.arrayDimensions; */
    /*     attr.arrayDimensionsSize = -1; */
    /*     attr.arrayDimensions = UA_NULL; */
    /* } */

    // don't use the extra type id. This comes from the value.
    /* if(attr.specifiedAttributes & UA_NODEATTRIBUTESMASK_DATATYPE || */
    /*    attr.specifiedAttributes & UA_NODEATTRIBUTESMASK_OBJECTTYPEORDATATYPE) { */
    /*     vnode->dataType = attr.dataType; */
    /*     UA_NodeId_init(&attr.dataType); */
    /* } */

    if(attr.specifiedAttributes & UA_NODEATTRIBUTESMASK_VALUE) {
        vnode->value.variant = attr.value;
        UA_Variant_init(&attr.value);
    }

    UA_VariableAttributes_deleteMembers(&attr);

    *new_node = (UA_Node*)vnode;
    return UA_STATUSCODE_GOOD;
}

static UA_StatusCode parseObjectNode(const UA_ExtensionObject *attributes, UA_Node **new_node) {
    if(attributes->typeId.identifier.numeric !=
       UA_TYPES[UA_TYPES_OBJECTATTRIBUTES].typeId.identifier.numeric + UA_ENCODINGOFFSET_BINARY)
        return UA_STATUSCODE_BADNODEATTRIBUTESINVALID;

    UA_ObjectAttributes attr;
    size_t pos = 0;
    // todo return more informative error codes from decodeBinary
    if (UA_ObjectAttributes_decodeBinary(&attributes->body, &pos, &attr) != UA_STATUSCODE_GOOD)
        return UA_STATUSCODE_BADNODEATTRIBUTESINVALID;
    UA_ObjectNode *vnode = UA_ObjectNode_new();
    if(!vnode) {
        UA_ObjectAttributes_deleteMembers(&attr);
        return UA_STATUSCODE_BADOUTOFMEMORY;
    }

    copyStandardAttributes((UA_Node*)vnode, (UA_NodeAttributes*)&attr);
    if(attr.specifiedAttributes & UA_NODEATTRIBUTESMASK_EVENTNOTIFIER)
      vnode->eventNotifier = attr.eventNotifier;
    UA_ObjectAttributes_deleteMembers(&attr);
    *new_node = (UA_Node*) vnode;
    return UA_STATUSCODE_GOOD;
}

static UA_StatusCode parseReferenceTypeNode(const UA_ExtensionObject *attributes, UA_Node **new_node) {
    UA_ReferenceTypeAttributes attr;
    size_t pos = 0;
    // todo return more informative error codes from decodeBinary
    if(UA_ReferenceTypeAttributes_decodeBinary(&attributes->body, &pos, &attr) != UA_STATUSCODE_GOOD)
        return UA_STATUSCODE_BADNODEATTRIBUTESINVALID;
    UA_ReferenceTypeNode *vnode = UA_ReferenceTypeNode_new();
    if(!vnode) {
        UA_ReferenceTypeAttributes_deleteMembers(&attr);
        return UA_STATUSCODE_BADOUTOFMEMORY;
    }

    copyStandardAttributes((UA_Node*)vnode, (UA_NodeAttributes*)&attr);
    if(attr.specifiedAttributes & UA_NODEATTRIBUTESMASK_ISABSTRACT)
        vnode->isAbstract = attr.isAbstract;
    if(attr.specifiedAttributes & UA_NODEATTRIBUTESMASK_SYMMETRIC)
        vnode->symmetric = attr.symmetric;
    if(attr.specifiedAttributes & UA_NODEATTRIBUTESMASK_INVERSENAME) {
        vnode->inverseName = attr.inverseName;
        attr.inverseName.text.length = -1;
        attr.inverseName.text.data = UA_NULL;
        attr.inverseName.locale.length = -1;
        attr.inverseName.locale.data = UA_NULL;
    }
    UA_ReferenceTypeAttributes_deleteMembers(&attr);
    *new_node = (UA_Node*) vnode;
    return UA_STATUSCODE_GOOD;
}

static UA_StatusCode parseObjectTypeNode(const UA_ExtensionObject *attributes, UA_Node **new_node) {
    UA_ObjectTypeAttributes attr;
    size_t pos = 0;
    // todo return more informative error codes from decodeBinary
    if(UA_ObjectTypeAttributes_decodeBinary(&attributes->body, &pos, &attr) != UA_STATUSCODE_GOOD)
        return UA_STATUSCODE_BADNODEATTRIBUTESINVALID;
    UA_ObjectTypeNode *vnode = UA_ObjectTypeNode_new();
    if(!vnode) {
        UA_ObjectTypeAttributes_deleteMembers(&attr);
        return UA_STATUSCODE_BADOUTOFMEMORY;
    }
    
    copyStandardAttributes((UA_Node*)vnode, (UA_NodeAttributes*)&attr);
    if(attr.specifiedAttributes & UA_NODEATTRIBUTESMASK_ISABSTRACT)
        vnode->isAbstract = attr.isAbstract;

    UA_ObjectTypeAttributes_deleteMembers(&attr);
    *new_node = (UA_Node*) vnode;
    return UA_STATUSCODE_GOOD;
}

static UA_StatusCode parseViewNode(const UA_ExtensionObject *attributes, UA_Node **new_node) {
    UA_ViewAttributes attr;
    size_t pos = 0;

    // todo return more informative error codes from decodeBinary
    if(UA_ViewAttributes_decodeBinary(&attributes->body, &pos, &attr) != UA_STATUSCODE_GOOD)
        return UA_STATUSCODE_BADNODEATTRIBUTESINVALID;
    UA_ViewNode *vnode = UA_ViewNode_new();
    if(!vnode) {
        UA_ViewAttributes_deleteMembers(&attr);
        return UA_STATUSCODE_BADOUTOFMEMORY;
    }

    copyStandardAttributes((UA_Node*)vnode, (UA_NodeAttributes*)&attr);
    if(attr.specifiedAttributes & UA_NODEATTRIBUTESMASK_CONTAINSNOLOOPS)
        vnode->containsNoLoops = attr.containsNoLoops;
    if(attr.specifiedAttributes & UA_NODEATTRIBUTESMASK_EVENTNOTIFIER)
        vnode->eventNotifier = attr.eventNotifier;

    UA_ViewAttributes_deleteMembers(&attr);
    *new_node = (UA_Node*) vnode;
    return UA_STATUSCODE_GOOD;
}

/************/
/* Add Node */
/************/

static void Service_AddNodes_single(UA_Server *server, UA_Session *session, const UA_AddNodesItem *item,
                                    UA_AddNodesResult *result) {
    // adding nodes to ns0 is not allowed over the wire
    if(item->requestedNewNodeId.nodeId.namespaceIndex == 0) {
        result->statusCode = UA_STATUSCODE_BADNODEIDREJECTED;
        return;
    }

    // parse the node
    UA_Node *node = UA_NULL;

    switch (item->nodeClass) {
    case UA_NODECLASS_OBJECT:
        result->statusCode = parseObjectNode(&item->nodeAttributes, &node);
        break;
    case UA_NODECLASS_OBJECTTYPE:
        result->statusCode = parseObjectTypeNode(&item->nodeAttributes, &node);
        break;
    case UA_NODECLASS_REFERENCETYPE:
        result->statusCode = parseReferenceTypeNode(&item->nodeAttributes, &node);
        break;
    case UA_NODECLASS_VARIABLE:
        result->statusCode = parseVariableNode(&item->nodeAttributes, &node);
        break;
    default:
        result->statusCode = UA_STATUSCODE_BADNOTIMPLEMENTED;
    }

    if(result->statusCode != UA_STATUSCODE_GOOD)
        return;
    
    // The BrowseName was not included with the NodeAttribute ExtensionObject
    UA_QualifiedName_init(&(node->browseName));
    UA_QualifiedName_copy(&(item->browseName), &(node->browseName));
    UA_NodeId_copy(&item->requestedNewNodeId.nodeId, &node->nodeId);
    
    // add the node
    *result = UA_Server_addNodeWithSession(server, session, node, item->parentNodeId,
                                           item->referenceTypeId);
    if(result->statusCode != UA_STATUSCODE_GOOD) {
        switch (node->nodeClass) {
        case UA_NODECLASS_OBJECT:
            UA_ObjectNode_delete((UA_ObjectNode*)node);
            break;
        case UA_NODECLASS_OBJECTTYPE:
            UA_ObjectTypeNode_delete((UA_ObjectTypeNode*)node);
            break;
        case UA_NODECLASS_REFERENCETYPE:
            UA_ReferenceTypeNode_delete((UA_ReferenceTypeNode*)node);
            break;
        case UA_NODECLASS_VARIABLE:
            UA_VariableNode_delete((UA_VariableNode*)node);
            break;
        default:
            UA_assert(UA_FALSE);
        }
    }
}

void Service_AddNodes(UA_Server *server, UA_Session *session, const UA_AddNodesRequest *request,
                      UA_AddNodesResponse *response) {
    if(request->nodesToAddSize <= 0) {
        response->responseHeader.serviceResult = UA_STATUSCODE_BADNOTHINGTODO;
        return;
    }
    size_t size = request->nodesToAddSize;

    response->results = UA_Array_new(&UA_TYPES[UA_TYPES_ADDNODESRESULT], size);
    if(!response->results) {
        response->responseHeader.serviceResult = UA_STATUSCODE_BADOUTOFMEMORY;
        return;
    }
    
#ifdef UA_EXTERNAL_NAMESPACES
#ifdef _MSVC_VER
    UA_Boolean *isExternal = UA_alloca(size);
    UA_UInt32 *indices = UA_alloca(sizeof(UA_UInt32)*size);
#else
    UA_Boolean isExternal[size];
    UA_UInt32 indices[size];
#endif
    UA_memset(isExternal, UA_FALSE, sizeof(UA_Boolean) * size);
    for(size_t j = 0; j <server->externalNamespacesSize; j++) {
        size_t indexSize = 0;
        for(size_t i = 0;i < size;i++) {
            if(request->nodesToAdd[i].requestedNewNodeId.nodeId.namespaceIndex !=
               server->externalNamespaces[j].index)
                continue;
            isExternal[i] = UA_TRUE;
            indices[indexSize] = i;
            indexSize++;
        }
        if(indexSize == 0)
            continue;
        UA_ExternalNodeStore *ens = &server->externalNamespaces[j].externalNodeStore;
        ens->addNodes(ens->ensHandle, &request->requestHeader, request->nodesToAdd,
                      indices, indexSize, response->results, response->diagnosticInfos);
    }
#endif
    
    response->resultsSize = size;
    for(size_t i = 0;i < size;i++) {
#ifdef UA_EXTERNAL_NAMESPACES
        if(!isExternal[i])
#endif
            Service_AddNodes_single(server, session, &request->nodesToAdd[i], &response->results[i]);
    }
}

/******************/
/* Add References */
/******************/

void Service_AddReferences(UA_Server *server, UA_Session *session, const UA_AddReferencesRequest *request,
                           UA_AddReferencesResponse *response) {
	if(request->referencesToAddSize <= 0) {
		response->responseHeader.serviceResult = UA_STATUSCODE_BADNOTHINGTODO;
		return;
	}
    size_t size = request->referencesToAddSize;
	
    if(!(response->results = UA_malloc(sizeof(UA_StatusCode) * size))) {
		response->responseHeader.serviceResult = UA_STATUSCODE_BADOUTOFMEMORY;
		return;
	}
	response->resultsSize = size;
	UA_memset(response->results, UA_STATUSCODE_GOOD, sizeof(UA_StatusCode) * size);

#ifdef UA_EXTERNAL_NAMESPACES
#ifdef NO_ALLOCA
    UA_Boolean isExternal[size];
    UA_UInt32 indices[size];
#else
    UA_Boolean *isExternal = UA_alloca(sizeof(UA_Boolean) * size);
    UA_UInt32 *indices = UA_alloca(sizeof(UA_UInt32) * size);
#endif /*NO_ALLOCA */
    UA_memset(isExternal, UA_FALSE, sizeof(UA_Boolean) * size);
	for(size_t j = 0; j < server->externalNamespacesSize; j++) {
		size_t indicesSize = 0;
		for(size_t i = 0;i < size;i++) {
			if(request->referencesToAdd[i].sourceNodeId.namespaceIndex
					!= server->externalNamespaces[j].index)
				continue;
			isExternal[i] = UA_TRUE;
			indices[indicesSize] = i;
			indicesSize++;
		}
		if (indicesSize == 0)
			continue;
		UA_ExternalNodeStore *ens = &server->externalNamespaces[j].externalNodeStore;
		ens->addReferences(ens->ensHandle, &request->requestHeader, request->referencesToAdd,
                           indices, indicesSize, response->results, response->diagnosticInfos);
	}
#endif

	response->resultsSize = size;
	for(UA_Int32 i = 0; i < response->resultsSize; i++) {
#ifdef UA_EXTERNAL_NAMESPACES
		if(!isExternal[i])
#endif
			UA_Server_addReferenceWithSession(server, session, &request->referencesToAdd[i]);
	}
}

/***************/
/* Delete Node */
/***************/

static UA_StatusCode Service_DeleteNodes_single(UA_Server *server, UA_NodeId nodeId,
                                                UA_Boolean deleteReferences) {
  const UA_Node *delNode = UA_NodeStore_get(server->nodestore, &nodeId);
  if (!delNode)
    return UA_STATUSCODE_BADNODEIDINVALID;
  
  // Find and remove all References to this node if so requested.
  if(deleteReferences == UA_TRUE) {
    UA_DeleteReferencesItem *delItem = UA_DeleteReferencesItem_new();
    delItem->deleteBidirectional = UA_TRUE; // WARNING: Current semantics in deleteOneWayReference is 'delete forward or inverse'
    UA_NodeId_copy(&nodeId, &delItem->targetNodeId.nodeId);
    
    for(int i=0; i<delNode->referencesSize; i++) {
      UA_NodeId_copy(&delNode->references[i].targetId.nodeId, &delItem->sourceNodeId);
      
      UA_NodeId_deleteMembers(&delItem->sourceNodeId);
    }
    
    UA_DeleteReferencesItem_delete(delItem);
  }
  
  UA_NodeStore_release(delNode);
  return UA_NodeStore_remove(server->nodestore, &nodeId);
}

void Service_DeleteNodes(UA_Server *server, UA_Session *session, const UA_DeleteNodesRequest *request,
                         UA_DeleteNodesResponse *response) {
  UA_StatusCode retval = UA_STATUSCODE_BADSERVICEUNSUPPORTED;
  response->resultsSize = request->nodesToDeleteSize;
  response->results = (UA_StatusCode *) UA_malloc(sizeof(UA_StatusCode) * request->nodesToDeleteSize);
  
  UA_DeleteNodesItem *item;
  for(int i=0; i<request->nodesToDeleteSize; i++) {
    item = &request->nodesToDelete[i];
    response->results[i] = Service_DeleteNodes_single(server, item->nodeId, item->deleteTargetReferences);
  }
  
  response->responseHeader.serviceResult = retval;
}

/********************/
/* Delete Reference */
/********************/

void Service_DeleteReferences(UA_Server *server, UA_Session *session,
                              const UA_DeleteReferencesRequest *request,
                              UA_DeleteReferencesResponse *response) {
  UA_StatusCode retval = UA_STATUSCODE_BADSERVICEUNSUPPORTED;
  response->responseHeader.serviceResult = retval;
}
