#pragma once

#include "../__dep__.h"
#include "../constants.h"
#include "../marshallable.h"
#include "pbft_rpc.h"

namespace janus {

typedef std::pair<uint64_t, locid_t> ViewSlotPair; 

class LogStore {
  private:
    std::map<ViewSlotPair, PreprepareMessage> preprepares_; 
    std::map<ViewSlotPair, std::map<locid_t, PrepareMessage>> prepares_; 
    std::map<ViewSlotPair, std::map<locid_t, CommitMessage>> commits_; 
    std::map<slotid_t, std::map<locid_t, CheckpointMessage>> checkpoints_;   
    std::map<uint64_t, std::map<locid_t, ViewChangeMessage>> view_changes_; 

  public: 
    bool HasPreprepare(uint64_t view, slotid_t slot) const;
    bool AddPreprepare(uint64_t view, slotid_t slot, const PreprepareMessage &mesg); 
    const PreprepareMessage &GetPreprepare(uint64_t view, slotid_t slot) const;

    bool HasPrepare(uint64_t view, slotid_t slot, locid_t server_id) const; 
    bool AddPrepare(uint64_t view, slotid_t slot, locid_t server_id, const PrepareMessage &mesg); 
    const std::map<locid_t, PrepareMessage> &GetPrepares(uint64_t view, slotid_t slot) const; 
    
    bool HasCommit(uint64_t view, slotid_t slot, locid_t server_id) const; 
    bool AddCommit(uint64_t view, slotid_t slot, locid_t server_id, const CommitMessage &mesg); 
    const std::map<locid_t, CommitMessage> &GetCommits(uint64_t view, slotid_t slot) const; 

    bool HasCheckpoint(slotid_t slot, locid_t server_id) const; 
    bool AddCheckpoint(slotid_t slot, locid_t server_id, const CheckpointMessage &mesg); 
    const std::map<locid_t, CheckpointMessage> &GetCheckpoints(slotid_t slot) const; 

    bool HasViewChange(uint64_t view, locid_t server_id) const; 
    bool AddViewChange(uint64_t view, locid_t server_id, const ViewChangeMessage &mesg); 
    const std::map<locid_t, ViewChangeMessage> &GetViewChanges(uint64_t view) const; 

    void ClearLog(uint64_t view, slotid_t slot); 
};

} // namespace janus
