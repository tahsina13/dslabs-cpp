#pragma once

#include "../__dep__.h"
#include "../constants.h"
#include "pbft_rpc.h"

class LogStore {
  private:
    std::map<slotid_t, PreprepareMessage> preprepares_; 
    std::map<slotid_t, std::map<locid_t, PrepareMessage>> prepares_; 
    std::map<slotid_t, std::map<locid_t, CommitMessage>> commits_; 
    std::map<slotid_t, std::map<locid_t, CheckpointMessage>> checkpoints_;   

  public: 
    bool HasPreprepare(slotid_t slot) const;
    bool AddPreprepare(slotid_t slot, const PreprepareMessage &mesg); 
    const PreprepareMessage &GetPreprepare(slotid_t slot) const;

    bool HasPrepare(slotid_t slot, svrid_t server_id) const; 
    bool AddPrepare(slotid_t slot, svrid_t server_id, const PrepareMessage &mesg); 
    uint64_t GetPrepareCount(slotid_t slot) const; 
    std::vector<PrepareMessage> GetPrepares(slotid_t slot) const;  
    
    bool AddCommit(slotid_t slot, svrid_t server_id, const CommitMessage &mesg); 
    bool HasCommit(slotid_t slot, svrid_t server_id) const; 
    uint64_t GetCommitCount(slotid_t slot) const; 
    std::vector<CommitMessage> GetCommits(slotid_t slot) const; 
};