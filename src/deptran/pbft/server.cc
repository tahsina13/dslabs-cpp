#include <openssl/rsa.h>

#include "server.h"
// #include "paxos_worker.h"
#include "exec.h"
#include "frame.h"
#include "coordinator.h"
#include "../classic/tpc_command.h"


namespace janus {

PbftServer::PbftServer(Frame * frame) : current_view_(0), 
                                        current_primary_(0),
                                        low_watermark_(0), 
                                        high_watermark_(MAX_REQUESTS_IN_TRANSIT),
                                        last_executed_(0),
                                        in_view_change_(false),
                                        md_(EVP_sha512()),
                                        privkey_auth_(md_, frame->site_info_->privkey) {
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
  std::lock_guard<std::recursive_mutex> lock(mtx_); 
  num_proxies_ = commo()->rpc_par_proxies_[partition_id_].size();
  uint64_t faults = (num_proxies_ - 1) / 3; 
  target_majority_ = 2 * faults + 1; 
  target_chkpt_majority_ = faults + 1; // for pBFT-PK model

  Config *config = Config::GetConfig(); 
  for (const auto &p : commo()->rpc_par_proxies_[partition_id_]) {
    const auto &pubkey = config->SitePubKeyById(p.first); 
    if (pubkey != nullptr) {
      pubkey_auth_.emplace(std::piecewise_construct,
                           std::forward_as_tuple(p.first),
                           std::forward_as_tuple(md_, pubkey)); 
    }
  }
  for (const auto &p : commo()->rpc_clients_) {
    const auto &pubkey = config->SitePubKeyById(p.first); 
    if (pubkey != nullptr) {
      pubkey_auth_.emplace(std::piecewise_construct,
                           std::forward_as_tuple(p.first),
                           std::forward_as_tuple(md_, pubkey)); 
    } 
  }
#ifdef PBFT_TEST_CORO
  pubkey_auth_.emplace(std::piecewise_construct,
                       std::forward_as_tuple(static_cast<siteid_t>(-1)),
                       std::forward_as_tuple(md_, static_cast<PbftFrame*>(frame_)->pubkey_)); 
#endif
}

bool PbftServer::Start(const shared_ptr<Marshallable>& cmd, 
                       const Request &req,
                       uint64_t *index, uint64_t *view) {
  std::lock_guard<std::recursive_mutex> lock(mtx_); 
  if (current_primary_ != site_id_ || !pubkey_auth_.count(req.client_id) || 
      !pubkey_auth_.at(req.client_id).VerifyRequest(cmd, req)) {
    return false; 
  }

  slotid_t seqno = low_watermark_ + 1; 
  while (seqno <= high_watermark_ && logstore_.HasPreprepare(seqno)) {
    seqno++; 
  }
  if (seqno > high_watermark_) {
    return false; 
  }
  requests_.emplace(std::piecewise_construct,
                    std::forward_as_tuple(seqno),
                    std::forward_as_tuple(cmd, req));

  std::string digest = privkey_auth_.GetDigest(*cmd); 
  PreprepareMessage preprepare = CreatePreprepare(current_view_, seqno, digest); 
  privkey_auth_.SignPreprepare(preprepare); 
  if (!logstore_.AddPreprepare(seqno, preprepare)) {
    return false; 
  }
  for (const auto& p : commo()->rpc_par_proxies_[partition_id_]) {
    if (p.first != loc_id_) {
      commo()->SendPreprepare(partition_id_, p.first, preprepare, cmd, req); 
    }
  }

  *index = seqno; 
  *view = current_view_; 
  return true;  
}

void PbftServer::GetState(bool *is_primary, uint64_t *view, uint64_t *chkpt_seqno) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  *is_primary = current_primary_ == loc_id_; 
  *view = current_view_; 
  *chkpt_seqno = low_watermark_; 
}

bool PbftServer::GetReply(uint64_t timestamp, cliid_t client_id, Reply *rep) {
  std::lock_guard<std::recursive_mutex> lock(mtx_); 
  auto it = replies_.find({timestamp, client_id}); 
  if (it == replies_.end()) {
    return false; 
  }
  *rep = it->second; 
  return true; 
}

PreprepareMessage PbftServer::CreatePreprepare(uint64_t view, slotid_t seqno, const std::string &digest) {
  return {
    .server_id = site_id_,
    .view = view,
    .seqno = seqno,
    .digest = digest,
  };
}

PrepareMessage PbftServer::CreatePrepare(uint64_t view, slotid_t seqno, const std::string &digest) {
  return {
    .server_id = site_id_,
    .view = view,
    .seqno = seqno,
    .digest = digest,
  }; 
}

CommitMessage PbftServer::CreateCommit(uint64_t view, slotid_t seqno, const std::string &digest) {
  return {
    .server_id = site_id_,
    .view = view,
    .seqno = seqno,
    .digest = digest,
  }; 
}

CheckpointMessage PbftServer::CreateCheckpoint(slotid_t chkpt_seqno, const std::string &chkpt_digest) {
  return {
    .server_id = site_id_,
    .chkpt_seqno = chkpt_seqno,
    .chkpt_digest = chkpt_digest,
  };
}

ViewChangeMessage PbftServer::CreateViewChange(uint64_t new_view) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  std::map<svrid_t, CheckpointMessage> checkpoints; 
  if (logstore_.GetCheckpointCount(low_watermark_) > 0) {
    checkpoints = logstore_.GetCheckpoints(low_watermark_);  
  }
  std::map<slotid_t, PreprepareMessage> preprepares; 
  std::map<slotid_t, std::map<svrid_t, PrepareMessage>> prepares; 
  for (const auto &request_entry : requests_) {
    const slotid_t slot = request_entry.first; 
    const ServerRequest &svr_req = request_entry.second;  
    if (svr_req.state >= RequestState::REQ_PREPARED) {
      preprepares[slot] = logstore_.GetPreprepare(slot);  
      prepares[slot] = logstore_.GetPrepares(slot); 
    }
  }  
  return {
    .server_id = site_id_,
    .new_view = new_view,
    .chkpt_seqno = low_watermark_,
    .checkpoints = checkpoints,
    .preprepares = preprepares,
    .prepares = prepares,
  }; 
}

NewViewMessage PbftServer::CreateNewView(uint64_t new_view, const std::map<svrid_t, ViewChangeMessage> &view_changes) {
  std::lock_guard<std::recursive_mutex> lock(mtx_); 
  slotid_t min_s = std::numeric_limits<slotid_t>::max(), max_s = 0;
  std::map<slotid_t, PreprepareMessage> preprepares; 
  for (const auto &view_change_entry : view_changes) {
    const ViewChangeMessage &view_change = view_change_entry.second; 
    bool is_chkpt_stable = (view_change.chkpt_seqno == 0) || // chkpt 0 stable by default 
      (view_change.checkpoints.size() >= target_chkpt_majority_);
    if (is_chkpt_stable) {
      min_s = std::min(min_s, view_change.chkpt_seqno);
    }
    for (const auto &preprepare_entry : view_change.preprepares) {
      const slotid_t slot = preprepare_entry.first;
      const PreprepareMessage &preprepare = preprepare_entry.second;
      bool is_prepared = view_change.prepares.count(slot) >= 0 && 
                         view_change.prepares.at(slot).size() >= target_majority_ - 1;
      if (is_prepared) {
        max_s = max(max_s, slot); 
        if (!preprepares.count(slot)) {
          preprepares[slot] = CreatePreprepare(new_view, slot, preprepare.digest); // no need to sign
        } 
      }
    }
  }
  for (slotid_t slot = min_s + 1; slot <= max_s; slot++) {
    if (!preprepares.count(slot)) {
      std::string digest; // TODO: generate digest from noop cmd
      preprepares[slot] = CreatePreprepare(new_view, slot, digest);
    }
  }
  return {
    .server_id = site_id_,
    .new_view = new_view,
    .view_changes = view_changes,
    .preprepares = preprepares,
  };
}

void PbftServer::HandlePreprepare(const PreprepareMessage &mesg) {
  std::lock_guard<std::recursive_mutex> lock(mtx_); 
  // Assumes preprepare message is valid and correct
  logstore_.AddPreprepare(mesg.seqno, mesg);  
  PrepareMessage prepare = CreatePrepare(current_view_, mesg.seqno, mesg.digest); 
  privkey_auth_.SignPrepare(prepare);
  for (const auto& p : commo()->rpc_par_proxies_[partition_id_]) {
    if (p.first != site_id_) {
      commo()->SendPrepare(partition_id_, p.first, prepare); 
    }
  }
  HandlePrepare(prepare);
}

void PbftServer::HandlePrepare(const PrepareMessage &mesg) {
  std::lock_guard<std::recursive_mutex> lock(mtx_); 
  // Assumes prepare message is valid and correct
  logstore_.AddPrepare(mesg.seqno, mesg.server_id, mesg); 
  bool has_majority = logstore_.GetPrepareCount(mesg.seqno) >= target_majority_;
  if (has_majority) {
    ServerRequest &svr_req = requests_.at(mesg.seqno); 
    svr_req.state = RequestState::REQ_PREPARED; 
    CommitMessage commit = CreateCommit(current_view_, mesg.seqno, mesg.digest); 
    privkey_auth_.SignCommit(commit); 
    for (auto p : commo()->rpc_par_proxies_[partition_id_]) {
      if (p.first != site_id_) {
        commo()->SendCommit(partition_id_, p.first, commit); 
      }
    }
    HandleCommit(commit); 
  }
}

void PbftServer::HandleCommit(const CommitMessage &mesg) {
  std::lock_guard<std::recursive_mutex> lock(mtx_); 
  // Assumes commit message is valid and correct
  logstore_.AddCommit(mesg.seqno, mesg.server_id, mesg); 
  bool has_majority = logstore_.GetCommitCount(mesg.seqno) >= target_majority_;
  if (has_majority) {
    ServerRequest &svr_req = requests_.at(mesg.seqno);
    svr_req.state = RequestState::REQ_COMMITTED; 
    while (requests_.count(last_executed_ + 1)) {
      svr_req = requests_.at(last_executed_ + 1); 
      if (svr_req.state == RequestState::REQ_COMMITTED) {
        Reply rep {
          .view = current_view_,
          .timestamp = svr_req.req.timestamp,
          .client_id = svr_req.req.client_id,
          .server_id = site_id_,
          .reply = app_next_(*svr_req.cmd),
        }; 
        privkey_auth_.SignReply(rep); 
        replies_.emplace(std::piecewise_construct,
                          std::forward_as_tuple(rep.timestamp, svr_req.req.client_id),
                          std::forward_as_tuple(rep));
        svr_req.state = RequestState::REQ_EXECUTED; 
        last_executed_++; 
      } else if (svr_req.state == RequestState::REQ_EXECUTED) {
        last_executed_++; 
      } else {
        break; 
      }
      if (last_executed_ % CHKPT_INTERVAL == 0) {
        CheckpointMessage chkpt = CreateCheckpoint(last_executed_, make_chkpt_(last_executed_));  
        privkey_auth_.SignCheckpoint(chkpt);  
        for (const auto &p : commo()->rpc_par_proxies_[partition_id_]) {
          if (p.first != site_id_) {
            commo()->SendCheckpoint(partition_id_, p.first, chkpt); 
          }
        }
        HandleCheckpoint(chkpt); 
      }
    }
  }
}

void PbftServer::HandleCheckpoint(const CheckpointMessage &mesg) {
  std::lock_guard<std::recursive_mutex> lock(mtx_); 
  if (logstore_.GetCheckpointCount(mesg.chkpt_seqno) > 0) {
    auto chkpt_digest = logstore_.GetCheckpoints(mesg.chkpt_seqno).begin()->second.chkpt_digest;
    if (chkpt_digest != mesg.chkpt_digest) {
      return; 
    }
  }
  logstore_.AddCheckpoint(mesg.chkpt_seqno, mesg.server_id, mesg); 
  bool has_majority = logstore_.GetCheckpointCount(mesg.chkpt_seqno) >= target_chkpt_majority_; 
  if (has_majority && mesg.chkpt_seqno > low_watermark_) {
    low_watermark_ = mesg.chkpt_seqno; 
    high_watermark_ = low_watermark_ + MAX_REQUESTS_IN_TRANSIT;
    logstore_.ClearLog(current_view_, low_watermark_); 
    requests_.erase(requests_.begin(), requests_.upper_bound(low_watermark_)); 
  }
}

void PbftServer::HandleViewChange(const ViewChangeMessage &mesg) {
  std::lock_guard<std::recursive_mutex> lock(mtx_); 
  in_view_change_ = true; 
  logstore_.AddViewChange(mesg.new_view, mesg.server_id, mesg); 
  bool has_majority = logstore_.GetViewChangeCount(mesg.new_view) >= target_majority_ - 1; 
  if ((current_primary_ == loc_id_) && has_majority) {
    if (logstore_.HasViewChange(mesg.new_view, site_id_) == 0) {
      logstore_.AddViewChange(mesg.new_view, site_id_, CreateViewChange(mesg.new_view)); 
    }
    NewViewMessage new_view = CreateNewView(mesg.new_view, logstore_.GetViewChanges(mesg.new_view)); 
    for (const auto &p : commo()->rpc_par_proxies_[partition_id_]) {
      if (p.first != site_id_) {
        commo()->SendNewView(partition_id_, p.first, new_view); 
      }
    }
    HandleNewView(new_view); 
  } 
}

void PbftServer::HandleNewView(const NewViewMessage &mesg) {
  auto new_view = CreateNewView(mesg.new_view, mesg.view_changes);
  auto m_it = mesg.preprepares.begin(); 
  auto o_it = new_view.preprepares.begin(); 
  while (m_it != mesg.preprepares.end() && o_it != new_view.preprepares.end()) {
    const slotid_t m_slot = m_it->first, o_slot = o_it->first; 
    const PreprepareMessage &m_preprepare = m_it->second, &o_preprepare = o_it->second;
    if (m_slot != o_slot ||
        m_preprepare.server_id != o_preprepare.server_id ||
        m_preprepare.view != o_preprepare.view ||
        m_preprepare.seqno != o_preprepare.seqno ||
        m_preprepare.digest != o_preprepare.digest) {
      return; 
    }
    m_it++, o_it++; 
  }
  
  current_view_ = mesg.new_view;
  current_primary_ = (current_view_ % num_proxies_); 
  logstore_.ClearLog(current_view_, low_watermark_); 
  for (const auto &preprepare_entry : mesg.preprepares) {
    const slotid_t slot = preprepare_entry.first;
    const PreprepareMessage &preprepare = preprepare_entry.second;
    if (!requests_.count(slot)) {
      // TODO: figure out how to fetch requests from replicas
      // auto cmptr = std::make_shared<TpcNoopCommand>(); 
      // auto cmd = std::dynamic_pointer_cast<Marshallable>(cmptr);
      // requests_[slot] = {cmd, 0, (cliid_t)num_proxies_, REQ_INIT};
    }
    HandlePreprepare(preprepare); 
  }
  in_view_change_ = false;  
}

void PbftServer::OnPreprepare(const PreprepareMessage &mesg, 
                              const std::shared_ptr<Marshallable> &cmd,
                              const Request &req,
                              const function<void()> &cb) {
  std::lock_guard<std::recursive_mutex> lock(mtx_); 
  Authenticator &auth = pubkey_auth_.at(mesg.server_id); 
  if (in_view_change_ || !auth.VerifyPreprepare(mesg)) {
    cb(); 
    return; 
  }

  if (logstore_.HasPreprepare(mesg.seqno) || requests_.count(mesg.seqno) != 0) {
    cb(); 
    return; 
  }

  Authenticator &req_auth = pubkey_auth_.at(req.client_id);
  bool is_from_primary = mesg.server_id == current_primary_;
  bool is_same_view = mesg.view == current_view_; 
  bool is_seqno_valid = low_watermark_ < mesg.seqno && mesg.seqno <= high_watermark_; 
  bool is_digest_valid = mesg.digest == privkey_auth_.GetDigest(*cmd); 
  bool is_req_valid = req_auth.VerifyRequest(cmd, req); 
  if (is_from_primary && is_same_view && is_seqno_valid && is_digest_valid && is_req_valid) {
    requests_.emplace(std::piecewise_construct,
                      std::forward_as_tuple(mesg.seqno),
                      std::forward_as_tuple(cmd, req));
    HandlePreprepare(mesg);
  } 
  cb(); 
}

void PbftServer::OnPrepare(const PrepareMessage &mesg, const function<void()> &cb) {
  std::lock_guard<std::recursive_mutex> lock(mtx_); 
  Authenticator &auth = pubkey_auth_.at(mesg.server_id);
  if (in_view_change_ || !auth.VerifyPrepare(mesg)) {
    cb(); 
    return; 
  }

  if (!logstore_.HasPreprepare(mesg.seqno) || requests_.count(mesg.seqno) == 0) {
    cb(); 
    return; 
  }
  const PreprepareMessage &preprepare = logstore_.GetPreprepare(mesg.seqno);
  const ServerRequest &svr_req = requests_.at(mesg.seqno);

  bool is_from_primary = mesg.server_id == current_primary_; 
  bool is_same_view = mesg.view == current_view_; 
  bool is_seqno_valid = low_watermark_ < mesg.seqno && mesg.seqno <= high_watermark_; 
  bool is_digest_valid = mesg.digest == preprepare.digest; 
  bool is_matches = preprepare.view == mesg.view && 
                    preprepare.seqno == mesg.seqno && 
                    preprepare.digest == mesg.digest;
  bool is_req_init = svr_req.state == RequestState::REQ_INIT;
  if (!is_from_primary && is_same_view && is_seqno_valid && is_digest_valid && is_matches && is_req_init) {
    HandlePrepare(mesg); 
  }
  cb(); 
}

void PbftServer::OnCommit(const CommitMessage &mesg, const function<void()> &cb) {
  std::lock_guard<std::recursive_mutex> lock(mtx_); 
  Authenticator &auth = pubkey_auth_.at(mesg.server_id);
  if (in_view_change_ || !auth.VerifyCommit(mesg)) {
    cb(); 
    return; 
  }

  if (!logstore_.HasPreprepare(mesg.seqno) || requests_.count(mesg.seqno) == 0) {
    cb(); 
    return; 
  }
  const PreprepareMessage &preprepare = logstore_.GetPreprepare(mesg.seqno);
  const ServerRequest &svr_req = requests_.at(mesg.seqno);

  bool is_same_view = mesg.view == current_view_; 
  bool is_seqno_valid = low_watermark_ < mesg.seqno && mesg.seqno <= high_watermark_; 
  bool is_digest_valid = mesg.digest == preprepare.digest;
  bool is_matches = preprepare.view == mesg.view && 
                    preprepare.seqno == mesg.seqno && 
                    preprepare.digest == mesg.digest;
  bool is_req_prepared = svr_req.state == RequestState::REQ_PREPARED;
  if (is_same_view && is_seqno_valid && is_digest_valid && is_matches && is_req_prepared) {
    HandleCommit(mesg);
  }
  cb(); 
}

void PbftServer::OnCheckpoint(const CheckpointMessage &mesg, const function<void()> &cb) {
  std::lock_guard<std::recursive_mutex> lock(mtx_); 
  Authenticator &auth = pubkey_auth_.at(mesg.server_id);
  if (auth.VerifyCheckpoint(mesg)) {
    HandleCheckpoint(mesg);
  }
  cb();  
}

void PbftServer::OnViewChange(const ViewChangeMessage &mesg, const function<void()> &cb) {
  std::lock_guard<std::recursive_mutex> lock(mtx_); 
  Authenticator &auth = pubkey_auth_.at(mesg.server_id);
  if (auth.VerifyViewChange(mesg)) {
    HandleViewChange(mesg); 
  }
  cb(); 
}

void PbftServer::OnNewView(const NewViewMessage &mesg, const function<void()> &cb) {
  std::lock_guard<std::recursive_mutex> lock(mtx_); 
  Authenticator &auth = pubkey_auth_.at(mesg.server_id);
  if (auth.VerifyNewView(mesg)) {
    HandleNewView(mesg); 
  }
  cb(); 
}

/* Do not modify any code below here */

void PbftServer::Disconnect(const bool disconnect) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  verify(disconnected_ != disconnect);
  // global map of rpc_par_proxies_ values accessed by partition then by site
  static map<parid_t, map<svrid_t, map<svrid_t, vector<SiteProxyPair>>>> _proxies{};
  if (_proxies.find(partition_id_) == _proxies.end()) {
    _proxies[partition_id_] = {};
  }
  PbftCommo *c = (PbftCommo*) commo();
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
