#include "logstore.h"

namespace janus {

bool LogStore::HasPreprepare(uint64_t view, slotid_t slot) const {
  return preprepares_.count(std::make_pair(view, slot)) > 0; 
}

bool LogStore::AddPreprepare(uint64_t view, slotid_t slot, const PreprepareMessage &mesg) {
  if (HasPreprepare(view, slot)) {
    return false; 
  }
  preprepares_[std::make_pair(view, slot)] = mesg; 
  return true; 
}

const PreprepareMessage &LogStore::GetPreprepare(uint64_t view, slotid_t slot) const {
  verify(HasPreprepare(view, slot));
  return preprepares_.at(std::make_pair(view, slot)); 
}


bool LogStore::HasPrepare(uint64_t view, slotid_t slot, locid_t server_id) const {
  ViewSlotPair index = std::make_pair(view, slot); 
  return prepares_.count(index) > 0 && prepares_.at(index).count(server_id) > 0; 
} 

bool LogStore::AddPrepare(uint64_t view, slotid_t slot, locid_t server_id, const PrepareMessage &mesg) {
  if (HasPrepare(view, slot, server_id)) {
    return false; 
  }
  prepares_[std::make_pair(view, slot)][server_id] = mesg; 
  return true; 
}

const std::map<locid_t, PrepareMessage> &LogStore::GetPrepares(uint64_t view, slotid_t slot) const {
  ViewSlotPair index = std::make_pair(view, slot); 
  verify(prepares_.count(index) > 0);
  return prepares_.at(index); 
}

bool LogStore::HasCommit(uint64_t view, slotid_t slot, locid_t server_id) const {
  ViewSlotPair index = std::make_pair(view, slot); 
  return commits_.count(index) > 0 && commits_.at(index).count(server_id) > 0; 
}

bool LogStore::AddCommit(uint64_t view, slotid_t slot, locid_t server_id, const CommitMessage &mesg) {
  if (HasCommit(view, slot, server_id)) {
    return false; 
  }
  commits_[std::make_pair(view, slot)][server_id] = mesg; 
  return true; 
}

const std::map<locid_t, CommitMessage> &LogStore::GetCommits(uint64_t view, slotid_t slot) const {
  ViewSlotPair index = std::make_pair(view, slot); 
  verify(commits_.count(index) > 0);
  return commits_.at(index); 
}

bool LogStore::HasCheckpoint(slotid_t slot, locid_t server_id) const {
  return checkpoints_.count(slot) > 0 && checkpoints_.at(slot).count(server_id) > 0; 
}

bool LogStore::AddCheckpoint(slotid_t slot, locid_t server_id, const CheckpointMessage &mesg) {
  if (HasCheckpoint(slot, server_id)) {
    return false; 
  }
  checkpoints_[slot][server_id] = mesg; 
  return true; 
}

const std::map<locid_t, CheckpointMessage> &LogStore::GetCheckpoints(slotid_t slot) const {
  verify(checkpoints_.count(slot) > 0);
  return checkpoints_.at(slot); 
}

bool LogStore::HasViewChange(uint64_t view, locid_t server_id) const {
  return view_changes_.count(view) > 0 && view_changes_.at(view).count(server_id) > 0; 
}

bool LogStore::AddViewChange(uint64_t view, locid_t server_id, const ViewChangeMessage &mesg) {
  if (HasViewChange(view, server_id)) {
    return false; 
  }
  view_changes_[view][server_id] = mesg; 
  return true; 
}

const std::map<locid_t, ViewChangeMessage> &LogStore::GetViewChanges(uint64_t view) const {
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
  ViewSlotPair view_slot_pair = std::make_pair(view, slot); 
  preprepares_.erase(preprepares_.begin(), preprepares_.upper_bound(view_slot_pair)); 
  prepares_.erase(prepares_.begin(), prepares_.upper_bound(view_slot_pair)); 
  commits_.erase(commits_.begin(), commits_.upper_bound(view_slot_pair));  
  checkpoints_.erase(checkpoints_.begin(), checkpoints_.upper_bound(slot)); 
  view_changes_.erase(view_changes_.begin(), view_changes_.upper_bound(view)); 
}

} // namespace janus
