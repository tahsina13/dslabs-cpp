#include "logstore.h"

bool LogStore::HasPreprepare(slotid_t slot) const {
  return preprepares_.count(slot) > 0; 
}

bool LogStore::AddPreprepare(slotid_t slot, const PreprepareMessage &mesg) {
  if (HasPreprepare(slot)) {
    return false; 
  }
  preprepares_[slot] = mesg; 
  return true; 
}

const PreprepareMessage &LogStore::GetPreprepare(slotid_t slot) const {
  verify(HasPreprepare(slot));
  return preprepares_.at(slot); 
}


bool LogStore::HasPrepare(slotid_t slot, svrid_t server_id) const {
  return prepares_.count(slot) > 0 && prepares_.at(slot).count(server_id) > 0; 
} 

bool LogStore::AddPrepare(slotid_t slot, svrid_t server_id, const PrepareMessage &mesg) {
  if (HasPrepare(slot, server_id)) {
    return false; 
  }
  prepares_[slot][server_id] = mesg; 
  return true; 
}

uint64_t LogStore::GetPrepareCount(slotid_t slot) const {
  if (prepares_.count(slot) == 0) {
    return 0; 
  }
  return prepares_.at(slot).size(); 
}

std::vector<PrepareMessage> LogStore::GetPrepares(slotid_t slot) const {
  std::vector<PrepareMessage> res; 
  if (prepares_.count(slot)) {
    for (auto &kv : prepares_.at(slot)) {
      res.push_back(kv.second); 
    }
  }
  return res; 
}

bool LogStore::HasCommit(slotid_t slot, svrid_t server_id) const {
  return commits_.count(slot) > 0 && commits_.at(slot).count(server_id) > 0; 
}

bool LogStore::AddCommit(slotid_t slot, svrid_t server_id, const CommitMessage &mesg) {
  if (HasCommit(slot, server_id)) {
    return false; 
  }
  commits_[slot][server_id] = mesg; 
  return true; 
}

uint64_t LogStore::GetCommitCount(slotid_t slot) const {
  if (commits_.count(slot) == 0) {
    return 0; 
  }
  return commits_.at(slot).size(); 
}

std::vector<CommitMessage> LogStore::GetCommits(slotid_t slot) const {
  std::vector<CommitMessage> res;
  if (commits_.count(slot)) {
    for (auto &kv : commits_.at(slot)) {
      res.push_back(kv.second); 
    }
  }
  return res; 
}
