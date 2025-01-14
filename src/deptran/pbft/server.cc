

#include "server.h"
// #include "paxos_worker.h"
#include "exec.h"
#include "frame.h"
#include "coordinator.h"
#include "../classic/tpc_command.h"


namespace janus {

PbftServer::PbftServer(Frame * frame) : current_view_(0), low_watermark_(0), high_watermark_(MAX_REQUESTS_IN_TRANSIT) {
  frame_ = frame ;
  /* Your code here for server initialization. Note that this function is 
     called in a different OS thread. Be careful about thread safety if 
     you want to initialize variables here. */

}

PbftServer::~PbftServer() {
  /* Your code here for server teardown */

}

void PbftServer::Setup() {
  /* Your code here for server setup. Due to the asynchronous nature of the 
     framework, this function could be called after a RPC handler is triggered. 
     Your code should be aware of that. This function is always called in the 
     same OS thread as the RPC handlers. */
  num_proxies_ = commo()->rpc_par_proxies_[partition_id_].size();
  uint64_t faults = (num_proxies_ - 1) / 3; 
  target_majority_ = 2 * faults + 1; 
}

void PbftServer::GetState(bool *is_primary, uint64_t *view) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  *is_primary = (current_view_ % num_proxies_ == loc_id_); 
  *view = current_view_; 
}

void PbftServer::OnPreprepare(const PreprepareMessage &mesg, 
                              std::shared_ptr<Marshallable> cmd,
                              uint64_t timestamp,
                              cliid_t client_id,
                              const function<void()> &cb) {
  /* Received by all servers */
  txnid_t tx_id = 0; 
  if (cmd->kind_ == MarshallDeputy::CMD_TPC_COMMIT) {
    auto cmdptr = dynamic_pointer_cast<TpcCommitCommand>(cmd); 
    tx_id = cmdptr->tx_id_; 
  }

  svrid_t primary_id = current_view_ % num_proxies_; 
  bool from_primary = mesg.server_id == primary_id; 
  bool is_seqno_valid = low_watermark_ < mesg.seqno && mesg.seqno <= high_watermark_; 
  bool is_valid_digest = mesg.digest == tx_id; 
  if (mesg.view != current_view_ || !from_primary || !is_seqno_valid || !is_valid_digest) {
    return; 
  }

  if (!logstore_.AddPreprepare(mesg.seqno, mesg)) {
    return; 
  }
  requests_[mesg.seqno] = {cmd, timestamp, client_id, REQ_INIT};

  PrepareMessage prepare = {mesg.view, mesg.seqno, mesg.digest, loc_id_};
  commo()->SendPrepare(partition_id_, primary_id, prepare); 
  cb(); 
}

void PbftServer::OnPrepare(const PrepareMessage &mesg, const function<void()> &cb) {
  /* Received by primary server only */
  bool is_seqno_valid = low_watermark_ < mesg.seqno && mesg.seqno <= high_watermark_; 
  if (mesg.view != current_view_ || !is_seqno_valid || !logstore_.HasPreprepare(mesg.seqno)) {
    return; 
  }

  Request &req = requests_[mesg.seqno]; 
  const PreprepareMessage &preprepare = logstore_.GetPreprepare(mesg.seqno);
  txnid_t tx_id = 0; 
  if (req.cmd->kind_ == MarshallDeputy::CMD_TPC_COMMIT) {
    auto cmdptr = dynamic_pointer_cast<TpcCommitCommand>(req.cmd); 
    tx_id = cmdptr->tx_id_; 
  }
  if (mesg.digest != tx_id || mesg.digest != preprepare.digest) {
    return; 
  }

  svrid_t primary_id = current_view_ % num_proxies_; 
  if (mesg.server_id != primary_id) {
    logstore_.AddPrepare(mesg.seqno, mesg.server_id, mesg); 
    bool is_prepared = logstore_.GetPrepareCount(mesg.seqno) >= target_majority_ - 1;
    if (req.state == RequestState::REQ_INIT && is_prepared) {
      auto prepares = logstore_.GetPrepares(mesg.seqno); 
      PreparedMessage prepared = {preprepare, prepares};  
      commo()->SendPrepared(partition_id_, -1, prepared); 
    }
  }
  cb(); 
}

void PbftServer::OnCommit(const CommitMessage &mesg, const function<void()> &cb) {
  /* Received by primary server only */
  bool is_seqno_valid = low_watermark_ < mesg.seqno && mesg.seqno <= high_watermark_; 
  if (mesg.view != current_view_ || !is_seqno_valid || !logstore_.HasPreprepare(mesg.seqno)) {
    return; 
  }

  Request &req = requests_[mesg.seqno]; 
  const PreprepareMessage &preprepare = logstore_.GetPreprepare(mesg.seqno);
  txnid_t tx_id = 0; 
  if (req.cmd->kind_ == MarshallDeputy::CMD_TPC_COMMIT) {
    auto cmdptr = dynamic_pointer_cast<TpcCommitCommand>(req.cmd); 
    tx_id = cmdptr->tx_id_; 
  }
  if (mesg.digest != tx_id || mesg.digest != preprepare.digest) {
    return; 
  }

  logstore_.AddCommit(mesg.seqno, mesg.server_id, mesg); 
  bool is_committed = logstore_.GetCommitCount(mesg.seqno) >= target_majority_;
  if (req.state == RequestState::REQ_PREPARED && is_committed) {
    auto prepares = logstore_.GetPrepares(mesg.seqno); 
    auto commits = logstore_.GetCommits(mesg.seqno); 
    CommittedMessage committed = {preprepare, prepares, commits}; 
    commo()->SendCommitted(partition_id_, -1, committed);
  }
  cb(); 
}

void PbftServer::OnPrepared(const PreparedMessage &mesg, const function<void()> &cb) {
  /* Received by all servers */
  svrid_t primary_id = current_view_ % num_proxies_; 
  bool from_primary = mesg.preprepare.server_id == primary_id; 
  bool is_seqno_valid = low_watermark_ < mesg.preprepare.seqno && mesg.preprepare.seqno <= high_watermark_;
  if (!is_seqno_valid || !from_primary || !logstore_.HasPreprepare(mesg.preprepare.seqno)) {
    return; 
  }

  Request &req = requests_[mesg.preprepare.seqno]; 
  const PreprepareMessage &preprepare = logstore_.GetPreprepare(mesg.preprepare.seqno);
  txnid_t tx_id = 0; 
  if (req.cmd->kind_ == MarshallDeputy::CMD_TPC_COMMIT) {
    auto cmdptr = dynamic_pointer_cast<TpcCommitCommand>(req.cmd); 
    tx_id = cmdptr->tx_id_; 
  }
  bool matches = mesg.preprepare.view == preprepare.view && 
    mesg.preprepare.seqno == preprepare.seqno && 
    mesg.preprepare.digest == preprepare.digest;
  for (const auto& prepare : mesg.prepares) {
    matches = matches && 
      prepare.view == mesg.preprepare.view && 
      prepare.seqno == mesg.preprepare.seqno && 
      prepare.digest == mesg.preprepare.digest;
  }
  if (mesg.preprepare.digest != tx_id || mesg.preprepare.digest != preprepare.digest || !matches) {
    return; 
  }

  for (const auto& prepare : mesg.prepares) {
    if (prepare.server_id != primary_id) {
      logstore_.AddPrepare(prepare.seqno, prepare.server_id, prepare);
    }
  }

  bool is_prepared = logstore_.GetPrepareCount(mesg.preprepare.seqno) >= target_majority_ - 1;
  if (req.state == RequestState::REQ_INIT && is_prepared) {
    req.state = RequestState::REQ_PREPARED; 
    CommitMessage commit = {mesg.preprepare.view, mesg.preprepare.seqno, mesg.preprepare.digest, loc_id_};
    commo()->SendCommit(partition_id_, primary_id, commit); 
  }
  cb();
}

void PbftServer::OnCommitted(const CommittedMessage &mesg, const function<void()> &cb) {
  /* Received by all servers */
  svrid_t primary_id = current_view_ % num_proxies_; 
  bool from_primary = mesg.preprepare.server_id == primary_id; 
  bool is_seqno_valid = low_watermark_ < mesg.preprepare.seqno && mesg.preprepare.seqno <= high_watermark_;
  if (!is_seqno_valid || !from_primary || !logstore_.HasPreprepare(mesg.preprepare.seqno)) {
    return; 
  }

  Request &req = requests_[mesg.preprepare.seqno]; 
  const PreprepareMessage &preprepare = logstore_.GetPreprepare(mesg.preprepare.seqno);
  txnid_t tx_id = 0; 
  if (req.cmd->kind_ == MarshallDeputy::CMD_TPC_COMMIT) {
    auto cmdptr = dynamic_pointer_cast<TpcCommitCommand>(req.cmd); 
    tx_id = cmdptr->tx_id_; 
  }
  bool matches = mesg.preprepare.view == preprepare.view && 
    mesg.preprepare.seqno == preprepare.seqno && 
    mesg.preprepare.digest == preprepare.digest;
  for (const auto& prepare : mesg.prepares) {
    matches = matches && 
      prepare.view == mesg.preprepare.view && 
      prepare.seqno == mesg.preprepare.seqno && 
      prepare.digest == mesg.preprepare.digest;
  }
  for (const auto& commit : mesg.commits) {
    matches = matches && 
      commit.view == mesg.preprepare.view && 
      commit.seqno == mesg.preprepare.seqno && 
      commit.digest == mesg.preprepare.digest;
  }
  if (mesg.preprepare.digest != tx_id || mesg.preprepare.digest != preprepare.digest || !matches) {
    return; 
  }

  for (const auto& prepare : mesg.prepares) {
    if (prepare.server_id != primary_id) {
      logstore_.AddPrepare(prepare.seqno, prepare.server_id, prepare);
    }
  }
  for (const auto& commit : mesg.commits) {
    logstore_.AddCommit(commit.seqno, commit.server_id, commit); 
  }
  

  bool has_majority = logstore_.GetCommitCount(mesg.preprepare.seqno) >= target_majority_;
  if (req.state == RequestState::REQ_PREPARED && has_majority) {
    app_next_(*req.cmd); 
    req.state = RequestState::REQ_COMMITTED;
    // TODO: send reply to client
  }
  cb();  
}

void PbftServer::OnCheckpoint(const CheckpointMessage &mesg, const function<void()> &cb) {
  // TODO: implement
  cb();  
}

void PbftServer::OnNewView(const NewViewMessage &mesg, const function<void()> &cb) {
  // TODO: implement
  cb(); 
}

void PbftServer::OnViewChange(const ViewChangeMessage &mesg, const function<void()> &cb) {
  // TODO: implement
  cb(); 
}

/* Do not modify any code below here */

void PbftServer::Disconnect(const bool disconnect) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  verify(disconnected_ != disconnect);
  // global map of rpc_par_proxies_ values accessed by partition then by site
  static map<parid_t, map<siteid_t, map<siteid_t, vector<SiteProxyPair>>>> _proxies{};
  if (_proxies.find(partition_id_) == _proxies.end()) {
    _proxies[partition_id_] = {};
  }
  RaftCommo *c = (RaftCommo*) commo();
  if (disconnect) {
    verify(_proxies[partition_id_][loc_id_].size() == 0);
    verify(c->rpc_par_proxies_.size() > 0);
    auto sz = c->rpc_par_proxies_.size();
    _proxies[partition_id_][loc_id_].insert(c->rpc_par_proxies_.begin(), c->rpc_par_proxies_.end());
    c->rpc_par_proxies_ = {};
    verify(_proxies[partition_id_][loc_id_].size() == sz);
    verify(c->rpc_par_proxies_.size() == 0);
  } else {
    verify(_proxies[partition_id_][loc_id_].size() > 0);
    auto sz = _proxies[partition_id_][loc_id_].size();
    c->rpc_par_proxies_ = {};
    c->rpc_par_proxies_.insert(_proxies[partition_id_][loc_id_].begin(), _proxies[partition_id_][loc_id_].end());
    _proxies[partition_id_][loc_id_] = {};
    verify(_proxies[partition_id_][loc_id_].size() == 0);
    verify(c->rpc_par_proxies_.size() == sz);
  }
  disconnected_ = disconnect;
}

bool PbftServer::IsDisconnected() {
  return disconnected_;
}

} // namespace janus
