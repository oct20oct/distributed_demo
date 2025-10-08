/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log,
                 Address *address) {
  for (int i = 0; i < 6; i++) {
    NULLADDR[i] = 0;
  }
  this->memberNode = member;
  this->emulNet = emul;
  this->log = log;
  this->par = params;
  this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into
 * the queue This function is called by a node to receive messages currently
 * waiting for it
 */
int MP1Node::recvLoop() {
  if (memberNode->bFailed) {
    return false;
  } else {
    // commets
    // cout<<"break point, node recieve and enqueue msg"<<endl;
    return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1,
                           &(memberNode->mp1q));
  }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
  Queue q;
  return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
  Address joinaddr;
  joinaddr = getJoinAddress();

  // Self booting routines
  if (initThisNode(&joinaddr) == -1) {
#ifdef DEBUGLOG
    log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
    exit(1);
  }

  if (!introduceSelfToGroup(&joinaddr)) {
    finishUpThisNode();
#ifdef DEBUGLOG
    log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
    exit(1);
  }

  return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
  /*
   * This function is partially implemented and may require changes
   */
  int id = *(int *)(&memberNode->addr.addr);
  int port = *(short *)(&memberNode->addr.addr[4]);

  memberNode->bFailed = false;
  memberNode->inited = true;
  memberNode->inGroup = false;
  // node is up!
  memberNode->nnb = 0;
  memberNode->heartbeat = 0;
  memberNode->pingCounter = TFAIL;
  memberNode->timeOutCounter = -1;
  initMemberListTable(memberNode);

  return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
  MessageHdr *msg;
#ifdef DEBUGLOG
  static char s[1024];
#endif

  if (0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr),
                  sizeof(memberNode->addr.addr))) {
    // I am the group booter (first process to join the group). Boot up the
    // group
#ifdef DEBUGLOG
    log->LOG(&memberNode->addr, "Starting up group...");
#endif
    memberNode->inGroup = true;
  } else {
    size_t msgsize =
        sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
    msg = (MessageHdr *)malloc(msgsize * sizeof(char));

    // create JOINREQ message: format of data is {struct Address myaddr}
    msg->msgType = JOINREQ;
    memcpy((char *)(msg + 1), &memberNode->addr.addr,
           sizeof(memberNode->addr.addr));
    memcpy((char *)(msg + 1) + 1 + sizeof(memberNode->addr.addr),
           &memberNode->heartbeat, sizeof(long));

#ifdef DEBUGLOG
    sprintf(s, "Trying to join...");
    log->LOG(&memberNode->addr, s);
#endif

    // send JOINREQ message to introducer member
    emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);

    free(msg);
  }

  return 1;
}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode() {
  memberNode->bFailed = true;
  memberNode->inited = false;
  memberNode->inGroup = false;
  memberNode->memberList.clear();
  memberNode->nnb = 0;
  memberNode->heartbeat = 0;
  memberNode->pingCounter = TFAIL;
  memberNode->timeOutCounter = -1;
  while (!memberNode->mp1q.empty()) {
    memberNode->mp1q.pop();
  }
  return 1;
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform
 * membership protocol duties
 */
void MP1Node::nodeLoop() {
  if (memberNode->bFailed) {
    finishUpThisNode();
    return;
  }

  // Check my messages
  checkMessages();

  // Wait until you're in the group...
  if (!memberNode->inGroup) {
    return;
  }

  // ...then jump in and share your responsibilites!
  nodeLoopOps();

  return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message
 * handler
 */
void MP1Node::checkMessages() {
  void *ptr;
  int size;

  // Pop waiting messages from memberNode's mp1q
  while (!memberNode->mp1q.empty()) {
    ptr = memberNode->mp1q.front().elt;
    size = memberNode->mp1q.front().size;
    //

    memberNode->mp1q.pop();
    recvCallBack((void *)memberNode, (char *)ptr, size);
  }
  return;
}

/**
 * FUNCTION NAME: unpackMessage
 *
 * DESCRIPTION: Helper function to unpack message content
 */
void unpackMessage(char *data, Address &addr, long &heartbeat) {
  // Unpack the address from the message.
  memcpy(&addr.addr, data, sizeof(Address));

  // Unpack the heartbeat from the message.
  // We add 1 to the offset to account for the single-byte padding
  // in the original `introduceSelfToGroup` function.
  memcpy(&heartbeat, data + sizeof(Address) + 1, sizeof(long));
}

// Helper function to pack a member list entry into a character array.
void packMemberListEntry(char *buffer, const MemberListEntry &entry) {
  int id = entry.getid();
  int port = entry.getport();
  long heartbeat = entry.getheartbeat();
  long timestamp = entry.gettimestamp();

  memcpy(buffer, &id, sizeof(int));
  memcpy(buffer + sizeof(int), &port, sizeof(int));
  memcpy(buffer + sizeof(int) * 2, &heartbeat, sizeof(long));
  memcpy(buffer + sizeof(int) * 2 + sizeof(long), &timestamp, sizeof(long));
}

// Helper function to unpack a member list entry from a character array.
void unpackMemberListEntry(char *buffer, MemberListEntry &entry) {
  int id, port;
  long heartbeat, timestamp;

  memcpy(&id, buffer, sizeof(int));
  memcpy(&port, buffer + sizeof(int), sizeof(int));
  memcpy(&heartbeat, buffer + sizeof(int) * 2, sizeof(long));
  memcpy(&timestamp, buffer + sizeof(int) * 2 + sizeof(long), sizeof(long));

  entry.setid(id);
  entry.setport(port);
  entry.setheartbeat(heartbeat);
  entry.settimestamp(timestamp);
}

/**
 * FUNCTION NAME: unpackGossipMessage
 *
 * DESCRIPTION: Helper function to unpack a list of MemberListEntry from the
 * payload.
 */
void unpackGossipMessage(char *payload, int size,
                         std::vector<MemberListEntry> &receivedList) {
  // FIX: Use the explicit, calculated size, not sizeof(MemberListEntry)
  int entrySize = MEMBER_ENTRY_SIZE; // Use the global constant
  int numEntries = size / entrySize;
  receivedList.resize(numEntries);

  char *current_payload_ptr = payload;
  for (int i = 0; i < numEntries; ++i) {
    // Use your existing unpack helper
    unpackMemberListEntry(current_payload_ptr, receivedList[i]);
    current_payload_ptr += entrySize;
  }
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size) {
  // std::cout << "*this is debug msg, recvCallBack is called" << std::endl;

  // Use reinterpret_cast to safely access the message header.
  MessageHdr *msg = reinterpret_cast<MessageHdr *>(data);

  // Get a pointer to the payload data, which starts after the header.
  char *payload = reinterpret_cast<char *>(msg + 1);
  // Case 1: Node is in the group and receives a JOINREQ
  if (memberNode->inGroup && msg->msgType == JOINREQ) {
    Address receivedAddr;
    long receivedHeartbeat;

    // Use the new helper function for clean unpacking.
    unpackMessage(payload, receivedAddr, receivedHeartbeat);

    // std::cout << "Successfully unpacked JOINREQ message." << std::endl;
    // std::cout << "Received JOINREQ Address: " << receivedAddr.getAddress() <<
    // std::endl; std::cout << "Received Heartbeat: " << receivedHeartbeat <<
    // std::endl;

    // Correctly calculate the size for the outgoing reply message.
    size_t replySize =
        sizeof(MessageHdr) + sizeof(memberNode->addr.addr) + sizeof(long) + 1;
    MessageHdr *msgRep = (MessageHdr *)malloc(replySize);

    // Populate the reply message.
    msgRep->msgType = JOINREP;
    char *replyPayload = reinterpret_cast<char *>(msgRep + 1);
    memcpy(replyPayload, &memberNode->addr.addr, sizeof(memberNode->addr.addr));
    memcpy(replyPayload + 1 + sizeof(memberNode->addr.addr),
           &memberNode->heartbeat, sizeof(long));

    // Send the reply message to the new node.
    emulNet->ENsend(&memberNode->addr, &receivedAddr, (char *)msgRep,
                    replySize);

    // Free the memory allocated for the reply message.
    free(msgRep);
  }
  // Case 2: Node is not in the group and receives a JOINREP
  else if (!memberNode->inGroup && msg->msgType == JOINREP) {
    Address receivedAddr;
    long receivedHeartbeat;

    // Use the same helper function for unpacking.
    unpackMessage(payload, receivedAddr, receivedHeartbeat);

    // std::cout << "Successfully unpacked JOINREP message." << std::endl;
    // std::cout << "Received JOINREP Address: " << receivedAddr.getAddress() <<
    // std::endl; std::cout << "Received Heartbeat: " << receivedHeartbeat <<
    // std::endl;

    // Join the group by changing the status.
    memberNode->inGroup = true;

    // Add self and the introducer to the membership list.
    MemberListEntry selfEntry;
    selfEntry.setid(*(int *)(&memberNode->addr.addr));
    selfEntry.setheartbeat(memberNode->heartbeat);
    selfEntry.setport(*(short *)(&memberNode->addr.addr[4]));
    selfEntry.settimestamp(par->getcurrtime());
    memberNode->memberList.push_back(selfEntry);

    #ifdef DEBUGLOG
    // Log self-join using the specialized function, with self as both nodes
    log->logNodeAdd(&memberNode->addr, &memberNode->addr); 
    #endif

    // Introducer's entry
    MemberListEntry newEntry;
    newEntry.setid(*(int *)(&receivedAddr.addr));
    newEntry.setheartbeat(receivedHeartbeat);
    newEntry.setport(*(short *)(&receivedAddr.addr[4]));
    newEntry.settimestamp(par->getcurrtime());
    memberNode->memberList.push_back(newEntry);

    // Log the addition of the introducer (receivedAddr) to the list
    #ifdef DEBUGLOG
    log->logNodeAdd(&memberNode->addr, &receivedAddr);
    #endif
  }
  // case 3, on recieve membership list from memberNode, merge the list
  // replace the heartbeatcount to highest number, newest timestamp
  if (memberNode->inGroup && msg->msgType == GOSSIP) {
    std::vector<MemberListEntry> receivedList;
    unpackGossipMessage(payload, size - sizeof(MessageHdr), receivedList);

    // std::cout << "Successfully unpacked GOSSIP message." << std::endl;
    // std::cout << "Received membership list with " << receivedList.size() << "
    // entries." << std::endl;

    // Iterate through the received list and merge with the local list
    for (const auto &receivedEntry : receivedList) {
      // Find if the member already exists in our list
      auto it = std::find_if(
          memberNode->memberList.begin(), memberNode->memberList.end(),
          [&](const MemberListEntry &localEntry) {
            return localEntry.getid() == receivedEntry.getid() &&
                   localEntry.getport() == receivedEntry.getport();
          });
      // Member exists in the local list, 
      if (it != memberNode->memberList.end()) {
        
        // 1. Check if the received heartbeat is strictly greater
        if (receivedEntry.getheartbeat() > it->getheartbeat()) {
          it->setheartbeat(receivedEntry.getheartbeat());
          it->settimestamp(par->getcurrtime()); 
          // std::cout << "Updated local membership list with existing member: "
          // << it->getid() << ":" << it->getport() << std::endl;
        }
    } 
      // Member does not exist in the local list, add it
      else {
        // FIX: DO NOT use par->getcurrtime() here.
        // The timestamp for a NEW member should be the received timestamp (from
        // the message),
        //long receivedTimestamp = receivedEntry.gettimestamp();
        MemberListEntry newEntry = receivedEntry;
        newEntry.settimestamp(par->getcurrtime()); // <--- Potential fix here
        memberNode->memberList.push_back(newEntry);
// std::cout << "Added new member: " << newEntry.getid() << ":" <<
// newEntry.getport() << std::endl;

// Log the addition of a new member
#ifdef DEBUGLOG
        Address new_addr;

        // FIX: Copy both the 4-byte ID and the 2-byte Port
        *(int *)(&new_addr.addr) = newEntry.getid();
        *(short *)(&new_addr.addr[4]) = newEntry.getport();

        log->logNodeAdd(
            &memberNode->addr,
            &new_addr); 
#endif
      }
    }
  }

  return true;
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and
 * then delete the nodes Propagate your membership list
 */
void MP1Node::nodeLoopOps() {
  // Current time is used to check for timeouts.
  long current_time = par->getcurrtime();
  // Increment heartbeat for the current node
  memberNode->heartbeat++;

  // FIX: Find and update the local node's entry in the list
  for (auto &entry : memberNode->memberList) {
    // Check if the entry belongs to the current node
    if (entry.getid() == *(int *)(&memberNode->addr.addr) &&
        entry.getport() == *(short *)(&memberNode->addr.addr[4])) {
      entry.setheartbeat(memberNode->heartbeat);
      entry.settimestamp(current_time);
      break; // Found and updated self, exit loop
    }
  }

  // need to print out current timestamp and node # and heartbeat count
  // std::cout << "Current Time: " << current_time
  //           << ", Node: " << memberNode->addr.getAddress()
  //           << ", Heartbeat: " << memberNode->heartbeat << std::endl;

  // 1. Check for failed nodes and remove them
  auto it = memberNode->memberList.begin();
  while (it != memberNode->memberList.end()) {
    if (current_time - it->gettimestamp() > TREMOVE) {

// std::cout << "Current Time: " << current_time
//           << ", Member " << it->getid() << ":" << it->getport() << " has been
//           removed." << std::endl;
//  Log the removal of the failed member
#ifdef DEBUGLOG
      Address removed_addr;

      // FIX: Copy both the 4-byte ID and the 2-byte Port
      *(int *)(&removed_addr.addr) = it->getid();
      *(short *)(&removed_addr.addr[4]) = it->getport();

      log->logNodeRemove(&memberNode->addr, &removed_addr);
#endif

      it = memberNode->memberList.erase(it);

    } else if (current_time - it->gettimestamp() > TFAIL) {
      // A node that has failed is not immediately removed. It is marked for
      // removal. In this simplified example, we can simply log it.
      // std::cout << "Current Time: " << current_time
      //          << ", Member " << it->getid() << ":" << it->getport() << " has
      //          failed." << std::endl;
      ++it;
    } else {
      ++it;
    }
  }

  // 2. Propagate the membership list to a random neighbor
  if (!memberNode->memberList.empty()) {
    int random_index = rand() % memberNode->memberList.size();
    MemberListEntry &target_entry = memberNode->memberList[random_index];

    Address target_addr{std::to_string(target_entry.getid()) + ":" +
                        std::to_string(target_entry.getport())};

    // std::cout << "Propagating membership list to node " <<
    // target_addr.getAddress() << std::endl;

    // Pack the entire membership list into a message
    size_t entrySize = MEMBER_ENTRY_SIZE; // FIX: Use the global constant
    size_t payloadSize = memberNode->memberList.size() * entrySize;
    size_t msgSize = sizeof(MessageHdr) + payloadSize;

    MessageHdr *msg = (MessageHdr *)malloc(msgSize);
    msg->msgType = GOSSIP;
    char *payload = reinterpret_cast<char *>(msg + 1);

    char *current_payload_ptr = payload;
    for (const auto &entry : memberNode->memberList) {
      packMemberListEntry(current_payload_ptr, entry);
      current_payload_ptr += entrySize;
    }

    // Send the message
    emulNet->ENsend(&memberNode->addr, &target_addr, (char *)msg, msgSize);

    // Clean up
    free(msg);
  }
  return;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
  return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
  Address joinaddr;

  memset(&joinaddr, 0, sizeof(Address));
  *(int *)(&joinaddr.addr) = 1;
  *(short *)(&joinaddr.addr[4]) = 0;

  return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
  memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr) {
  printf("%d.%d.%d.%d:%d \n", addr->addr[0], addr->addr[1], addr->addr[2],
         addr->addr[3], *(short *)&addr->addr[4]);
}
