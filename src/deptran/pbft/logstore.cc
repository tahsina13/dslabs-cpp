#include "logstore.h"

namespace janus {

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

size_t LogStore::GetPrepareCount(slotid_t slot) const {
  if (prepares_.count(slot) == 0) {
    return 0; 
  }
  return prepares_.at(slot).size(); 
}

const std::map<svrid_t, PrepareMessage> &LogStore::GetPrepares(slotid_t slot) const {
  verify(prepares_.count(slot) > 0);
  return prepares_.at(slot); 
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

size_t LogStore::GetCommitCount(slotid_t slot) const {
  if (commits_.count(slot) == 0) {
    return 0; 
  }
  return commits_.at(slot).size(); 
}

const std::map<svrid_t, CommitMessage> &LogStore::GetCommits(slotid_t slot) const {
  verify(commits_.count(slot) > 0);
  return commits_.at(slot); 
}

bool LogStore::HasCheckpoint(slotid_t slot, svrid_t server_id) const {
  return checkpoints_.count(slot) > 0 && checkpoints_.at(slot).count(server_id) > 0; 
}

bool LogStore::AddCheckpoint(slotid_t slot, svrid_t server_id, const CheckpointMessage &mesg) {
  if (HasCheckpoint(slot, server_id)) {
    return false; 
  }
  checkpoints_[slot][server_id] = mesg; 
  return true; 
}

size_t LogStore::GetCheckpointCount(slotid_t slot) const {
  if (checkpoints_.count(slot) == 0) {
    return 0; 
  }
  return checkpoints_.at(slot).size(); 
}

const std::map<svrid_t, CheckpointMessage> &LogStore::GetCheckpoints(slotid_t slot) const {
  verify(checkpoints_.count(slot) > 0);
  return checkpoints_.at(slot); 
}

bool LogStore::HasViewChange(uint64_t view, svrid_t server_id) const {
  return view_changes_.count(view) > 0 && view_changes_.at(view).count(server_id) > 0; 
}

bool LogStore::AddViewChange(uint64_t view, svrid_t server_id, const ViewChangeMessage &mesg) {
  if (HasViewChange(view, server_id)) {
    return false; 
  }
  view_changes_[view][server_id] = mesg; 
  return true; 
}

size_t LogStore::GetViewChangeCount(uint64_t view) const {
  if (view_changes_.count(view) == 0) {
    return 0; 
  }
  return view_changes_.at(view).size(); 
}

const std::map<svrid_t, ViewChangeMessage> &LogStore::GetViewChanges(uint64_t view) const {
  verify(view_changes_.count(view) > 0);
  return view_changes_.at(view); 
}

/**
 * @brief Removes all messages with view number less than view and slot number less than slot. 
 * 
 * @param view The view number.
 * @param slot The slot number.
 */
void LogStore::ClearLog(uint64_t view, slotid_t slot) {
  preprepares_.erase(preprepares_.begin(), preprepares_.upper_bound(slot)); 
  prepares_.erase(prepares_.begin(), prepares_.upper_bound(slot)); 
  commits_.erase(commits_.begin(), commits_.upper_bound(slot));  
  checkpoints_.erase(checkpoints_.begin(), checkpoints_.upper_bound(slot)); 
  view_changes_.erase(view_changes_.begin(), view_changes_.upper_bound(view)); 
}

} // namespace janus
