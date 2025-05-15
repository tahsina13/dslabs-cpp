#pragma once

#include "../__dep__.h"
#include "../constants.h"
#include "../marshallable.h"
#include "pbft_rpc.h"

namespace janus {

class LogStore {
  private:
    std::map<slotid_t, PreprepareMessage> preprepares_; 
    std::map<slotid_t, std::map<svrid_t, PrepareMessage>> prepares_; 
    std::map<slotid_t, std::map<svrid_t, CommitMessage>> commits_; 
    std::map<slotid_t, std::map<svrid_t, CheckpointMessage>> checkpoints_;   
    std::map<uint64_t, std::map<svrid_t, ViewChangeMessage>> view_changes_; 

  public: 
    bool HasPreprepare(slotid_t slot) const;
    bool AddPreprepare(slotid_t slot, const PreprepareMessage &mesg); 
    const PreprepareMessage &GetPreprepare(slotid_t slot) const;

    bool HasPrepare(slotid_t slot, svrid_t server_id) const; 
    bool AddPrepare(slotid_t slot, svrid_t server_id, const PrepareMessage &mesg); 
    size_t GetPrepareCount(slotid_t slot) const; 
    const std::map<svrid_t, PrepareMessage> &GetPrepares(slotid_t slot) const; 
    
    bool HasCommit(slotid_t slot, svrid_t server_id) const; 
    bool AddCommit(slotid_t slot, svrid_t server_id, const CommitMessage &mesg); 
    size_t GetCommitCount(slotid_t slot) const; 
    const std::map<svrid_t, CommitMessage> &GetCommits(slotid_t slot) const; 

    bool HasCheckpoint(slotid_t slot, svrid_t server_id) const; 
    bool AddCheckpoint(slotid_t slot, svrid_t server_id, const CheckpointMessage &mesg); 
    size_t GetCheckpointCount(slotid_t slot) const; 
    const std::map<svrid_t, CheckpointMessage> &GetCheckpoints(slotid_t slot) const; 

    bool HasViewChange(uint64_t view, svrid_t server_id) const; 
    bool AddViewChange(uint64_t view, svrid_t server_id, const ViewChangeMessage &mesg); 
    size_t GetViewChangeCount(uint64_t view) const; 
    const std::map<svrid_t, ViewChangeMessage> &GetViewChanges(uint64_t view) const; 

    void ClearLog(uint64_t view, slotid_t slot); 
};

} // namespace janus
