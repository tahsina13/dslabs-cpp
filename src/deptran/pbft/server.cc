

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
                                        md_(EVP_sha3_512()) {
  frame_ = frame ;
  /* Your code here for server initialization. Note that this function is 
     called in a different OS thread. Be careful about thread safety if 
     you want to initialize variables here. */

  verify((mdctx_ = EVP_MD_CTX_new()) != NULL);
}

PbftServer::~PbftServer() {
  /* Your code here for server teardown */

  EVP_MD_CTX_free(mdctx_); 
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

bool PbftServer::Start(shared_ptr<Marshallable>& cmd, 
                       uint64_t timestamp, 
                       cliid_t client_id, 
                       uint64_t *index, 
                       uint64_t *view) {
  std::lock_guard<std::recursive_mutex> lock(mtx_); 
  if (current_primary_ != loc_id_) {
    return false; 
  }

  slotid_t seqno = low_watermark_ + 1; 
  while (seqno <= high_watermark_ && logstore_.HasPreprepare(current_view_, seqno)) {
    seqno++; 
  }
  if (seqno > high_watermark_) {
    return false; 
  }

  uint64_t tx_id = 0; 
  if (cmd->kind_ == MarshallDeputy::CMD_TPC_COMMIT) {
    auto cmdptr = dynamic_pointer_cast<TpcCommitCommand>(cmd); 
    tx_id = cmdptr->tx_id_; 
  }

  PreprepareMessage preprepare = CreatePreprepare(current_view_, seqno); 
  if (!logstore_.AddPreprepare(current_view_, seqno, preprepare)) {
    return false; 
  }
  requests_[seqno] = {
    .cmd = cmd,
    .timestamp = timestamp,
    .client_id = client_id,
    .state = RequestState::REQ_INIT,
  }; 

  for (const auto& p : commo()->rpc_par_proxies_[partition_id_]) {
    if (p.first != loc_id_) {
      commo()->SendPreprepare(partition_id_, p.first, preprepare, cmd, timestamp, client_id); 
    }
  }

  *index = seqno; 
  *view = current_view_; 
  return true;  
}

void PbftServer::GetState(bool *is_primary, uint64_t *view) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  *is_primary = current_primary_ == loc_id_; 
  *view = current_view_; 
}

bool PbftServer::GetReply(uint64_t timestamp, cliid_t client_id, string *reply) {
  std::lock_guard<std::recursive_mutex> lock(mtx_); 
  auto it = replies_.find({timestamp, client_id}); 
  if (it == replies_.end()) {
    return false; 
  }
  *reply = it->second; 
  return true; 
}

PreprepareMessage PbftServer::CreatePreprepare(uint64_t view, slotid_t seqno) {
  std::lock_guard<std::recursive_mutex> lock(mtx_); 
  unsigned char *raw_digest; 
  if (EVP_DigestInit_ex(mdctx_, md_, NULL) != 1) {
    Log_fatal("EVP_DigestInit_ex failed");
  }
  if (EVP_DigestUpdate(mdctx_, &loc_id_, sizeof(loc_id_)) != 1 ||
      EVP_DigestUpdate(mdctx_, &view, sizeof(view)) != 1 ||
      EVP_DigestUpdate(mdctx_, &seqno, sizeof(seqno)) != 1) {
    Log_fatal("EVP_DigestUpdate failed");
  }
  if ((raw_digest = (unsigned char *)OPENSSL_malloc(EVP_MD_size(md_))) == NULL) {
    Log_fatal("OPENSSL_malloc failed");
  }
  if (EVP_DigestFinal_ex(mdctx_, raw_digest, NULL) != 1) {
    Log_fatal("EVP_DigestFinal_ex failed");
  }
  std::string digest (reinterpret_cast<const char*>(raw_digest), EVP_MD_size(md_)); 
  OPENSSL_free(raw_digest);
  return {
    .server_id = loc_id_,
    .view = view,
    .seqno = seqno,
    .digest = digest,
  }; 
}

PrepareMessage PbftServer::CreatePrepare(uint64_t view, slotid_t seqno) {
  std::lock_guard<std::recursive_mutex> lock(mtx_); 
  unsigned char *raw_digest; 
  if (EVP_DigestInit_ex(mdctx_, md_, NULL) != 1) {
    Log_fatal("EVP_DigestInit_ex failed");
  }
  if (EVP_DigestUpdate(mdctx_, &loc_id_, sizeof(loc_id_)) != 1 ||
      EVP_DigestUpdate(mdctx_, &view, sizeof(view)) != 1 ||
      EVP_DigestUpdate(mdctx_, &seqno, sizeof(seqno)) != 1) {
    Log_fatal("EVP_DigestUpdate failed");
  }
  if ((raw_digest = (unsigned char *)OPENSSL_malloc(EVP_MD_size(md_))) == NULL) {
    Log_fatal("OPENSSL_malloc failed");
  }
  if (EVP_DigestFinal_ex(mdctx_, raw_digest, NULL) != 1) {
    Log_fatal("EVP_DigestFinal_ex failed");
  }
  std::string digest (reinterpret_cast<const char*>(raw_digest), EVP_MD_size(md_));
  OPENSSL_free(raw_digest);
  return {
    .server_id = loc_id_,
    .view = view,
    .seqno = seqno,
    .digest = digest,
  }; 
}

CommitMessage PbftServer::CreateCommit(uint64_t view, slotid_t seqno) {
  std::lock_guard<std::recursive_mutex> lock(mtx_); 
  unsigned char *raw_digest; 
  if (EVP_DigestInit_ex(mdctx_, md_, NULL) != 1) {
    Log_fatal("EVP_DigestInit_ex failed");
  }
  if (EVP_DigestUpdate(mdctx_, &loc_id_, sizeof(loc_id_)) != 1 ||
      EVP_DigestUpdate(mdctx_, &view, sizeof(view)) != 1 ||
      EVP_DigestUpdate(mdctx_, &seqno, sizeof(seqno)) != 1) {
    Log_fatal("EVP_DigestUpdate failed");
  }
  if ((raw_digest = (unsigned char *)OPENSSL_malloc(EVP_MD_size(md_))) == NULL) {
    Log_fatal("OPENSSL_malloc failed");
  }
  if (EVP_DigestFinal_ex(mdctx_, raw_digest, NULL) != 1) {
    Log_fatal("EVP_DigestFinal_ex failed");
  }
  std::string digest (reinterpret_cast<const char*>(raw_digest), EVP_MD_size(md_));
  OPENSSL_free(raw_digest);
  return {
    .server_id = loc_id_,
    .view = view,
    .seqno = seqno,
    .digest = digest,
  }; 
}

CheckpointMessage PbftServer::CreateCheckpoint(slotid_t ckpt_seqno) {
  std::lock_guard<std::recursive_mutex> lock(mtx_); 
  unsigned char *raw_digest; 
  if (EVP_DigestInit_ex(mdctx_, md_, NULL) != 1) {
    Log_fatal("EVP_DigestInit_ex failed");
  }
  if (EVP_DigestUpdate(mdctx_, &loc_id_, sizeof(loc_id_)) != 1 ||
      EVP_DigestUpdate(mdctx_, &low_watermark_, sizeof(low_watermark_)) != 1) {
    Log_fatal("EVP_DigestUpdate failed");
  }
  if ((raw_digest = (unsigned char *)OPENSSL_malloc(EVP_MD_size(md_))) == NULL) {
    Log_fatal("OPENSSL_malloc failed");
  }
  if (EVP_DigestFinal_ex(mdctx_, raw_digest, NULL) != 1) {
    Log_fatal("EVP_DigestFinal_ex failed");
  }
  std::string digest (reinterpret_cast<const char*>(raw_digest), EVP_MD_size(md_));
  OPENSSL_free(raw_digest);
  return {
    .server_id = loc_id_,
    .ckpt_seqno = ckpt_seqno,
    .digest = digest,
  }; 
}

ViewChangeMessage PbftServer::CreateViewChange(uint64_t new_view) {
  std::lock_guard<std::recursive_mutex> lock(mtx_); 
  auto checkpoints = logstore_.GetCheckpoints(low_watermark_); 
  std::map<slotid_t, PreprepareMessage> preprepares; 
  std::map<slotid_t, std::map<locid_t, PrepareMessage>> prepares; 
  for (const auto &[slot, req] : requests_) {
    if (req.state >= RequestState::REQ_PREPARED) {
      preprepares[slot] = logstore_.GetPreprepare(current_view_, slot);  
      prepares[slot] = logstore_.GetPrepares(current_view_, slot); 
    }
  }  

  unsigned char *raw_digest; 
  if (EVP_DigestInit_ex(mdctx_, md_, NULL) != 1) {
    Log_fatal("EVP_DigestInit_ex failed");
  }
  if (EVP_DigestUpdate(mdctx_, &loc_id_, sizeof(loc_id_)) != 1 ||
      EVP_DigestUpdate(mdctx_, &new_view, sizeof(new_view)) != 1 ||
      EVP_DigestUpdate(mdctx_, &low_watermark_, sizeof(low_watermark_)) != 1) {
    Log_fatal("EVP_DigestUpdate failed");
  }
  for (auto &[_, ckpt] : checkpoints) {
    if (EVP_DigestUpdate(mdctx_, ckpt.digest.data(), ckpt.digest.size()) != 1) {
      Log_fatal("EVP_DigestUpdate failed");
    }
  }
  for (auto &[_, preprepare] : preprepares) {
    if (EVP_DigestUpdate(mdctx_, preprepare.digest.data(), preprepare.digest.size()) != 1) {
      Log_fatal("EVP_DigestUpdate failed");
    }
  }
  for (auto &[_, prepare_map] : prepares) {
    for (auto &[_, prepare] : prepare_map) {
      if (EVP_DigestUpdate(mdctx_, prepare.digest.data(), prepare.digest.size()) != 1) {
        Log_fatal("EVP_DigestUpdate failed");
      }
    }
  }
  if ((raw_digest = (unsigned char *)OPENSSL_malloc(EVP_MD_size(md_))) == NULL) {
    Log_fatal("OPENSSL_malloc failed");
  }
  if (EVP_DigestFinal_ex(mdctx_, raw_digest, NULL) != 1) {
    Log_fatal("EVP_DigestFinal_ex failed");
  }
  std::string digest (reinterpret_cast<const char*>(raw_digest), EVP_MD_size(md_));
  OPENSSL_free(raw_digest);

  return {
    .server_id = loc_id_,
    .new_view = new_view,
    .ckpt_seqno = low_watermark_,
    .checkpoints = checkpoints,
    .preprepares = preprepares,
    .prepares = prepares,
    .digest = digest,
  }; 
}

NewViewMessage PbftServer::CreateNewView(uint64_t new_view, const std::map<locid_t, ViewChangeMessage> &view_changes) {
  std::lock_guard<std::recursive_mutex> lock(mtx_); 
  slotid_t min_s = std::numeric_limits<slotid_t>::max(), max_s = 0;
  std::map<slotid_t, PreprepareMessage> preprepares; 

  for (const auto &[_, view_change] : view_changes) {
    min_s = std::min(min_s, view_change.ckpt_seqno); 
    for (const auto &[slot, preprepare] : view_change.preprepares) {
      bool is_prepared = view_change.prepares.count(slot) >= 0 && 
                         view_change.prepares.at(slot).size() >= target_majority_;
      if (is_prepared) {
        max_s = max(max_s, slot); 
        if (!preprepares.count(slot)) {
          preprepares[slot] = CreatePreprepare(new_view, slot);
        } 
      }
    }
  }
  for (slotid_t slot = min_s + 1; slot <= max_s; slot++) {
    if (!preprepares.count(slot)) {
      preprepares[slot] = CreatePreprepare(new_view, slot);
    }
  }
  
  unsigned char *raw_digest; 
  if (EVP_DigestInit_ex(mdctx_, md_, NULL) != 1) {
    Log_fatal("EVP_DigestInit_ex failed");
  }
  if (EVP_DigestUpdate(mdctx_, &loc_id_, sizeof(loc_id_)) != 1 ||
      EVP_DigestUpdate(mdctx_, &new_view, sizeof(new_view)) != 1) {
    Log_fatal("EVP_DigestUpdate failed");
  }
  for (const auto &[_, view_change] : view_changes) {
    if (EVP_DigestUpdate(mdctx_, view_change.digest.data(), view_change.digest.size()) != 1) {
      Log_fatal("EVP_DigestUpdate failed");
    }
  }
  for (const auto &[_, preprepare] : preprepares) {
    if (EVP_DigestUpdate(mdctx_, preprepare.digest.data(), preprepare.digest.size()) != 1) {
      Log_fatal("EVP_DigestUpdate failed");
    }
  }
  if ((raw_digest = (unsigned char *)OPENSSL_malloc(EVP_MD_size(md_))) == NULL) {
    Log_fatal("OPENSSL_malloc failed");
  }
  if (EVP_DigestFinal_ex(mdctx_, raw_digest, NULL) != 1) {
    Log_fatal("EVP_DigestFinal_ex failed");
  }
  std::string digest (reinterpret_cast<const char*>(raw_digest), EVP_MD_size(md_));
  OPENSSL_free(raw_digest);

  return {
    .server_id = loc_id_,
    .new_view = new_view,
    .view_changes = view_changes,
    .preprepares = preprepares,
  }; 
}

void PbftServer::OnPreprepare(const PreprepareMessage &mesg, 
                              std::shared_ptr<Marshallable> cmd,
                              uint64_t timestamp,
                              cliid_t client_id,
                              const function<void()> &cb) {
  std::lock_guard<std::recursive_mutex> lock(mtx_); 
  bool is_same_view = mesg.view == current_view_; 
  bool is_from_primary = mesg.server_id == current_primary_; 
  bool is_seqno_valid = low_watermark_ < mesg.seqno && mesg.seqno <= high_watermark_; 
  bool is_digest_valid = true; // TODO: check signature of digest
  if (in_view_change_ || !is_same_view || !is_from_primary || !is_seqno_valid || !is_digest_valid) {
    return; 
  }

  if (!logstore_.AddPreprepare(mesg.view, mesg.seqno, mesg)) {
    return; 
  }
  requests_[mesg.seqno] = {
    .cmd = cmd,
    .timestamp = timestamp,
    .client_id = client_id,
    .state = RequestState::REQ_INIT,
  }; 

  PrepareMessage prepare = CreatePrepare(mesg.view, mesg.seqno); 
  for (const auto& p : commo()->rpc_par_proxies_[partition_id_]) {
    // TODO: don't send messages to self
    commo()->SendPrepare(partition_id_, p.first, prepare); 
  }
  cb(); 
}

void PbftServer::OnPrepare(const PrepareMessage &mesg, const function<void()> &cb) {
  std::lock_guard<std::recursive_mutex> lock(mtx_); 
  bool is_same_view = mesg.view == current_view_; 
  bool is_from_primary = mesg.server_id == current_primary_; 
  bool is_seqno_valid = low_watermark_ < mesg.seqno && mesg.seqno <= high_watermark_; 
  bool is_digest_valid = true; // TODO: check signature of digest
  if (in_view_change_ || !is_same_view || is_from_primary || !is_seqno_valid || !is_digest_valid || 
    !logstore_.HasPreprepare(mesg.view, mesg.seqno) || !requests_.count(mesg.seqno)) {
    return; 
  }

  Request &req = requests_[mesg.seqno]; // TODO: is request necessarily present for null requests? 

  logstore_.AddPrepare(mesg.view, mesg.seqno, mesg.server_id, mesg); 
  bool is_prepared = logstore_.GetPrepares(mesg.view, mesg.seqno).size() >= target_majority_ - 1;
  if (req.state == RequestState::REQ_INIT && is_prepared) {
    req.state = RequestState::REQ_PREPARED; 
    CommitMessage commit = CreateCommit(mesg.view, mesg.seqno); 
    for (auto p : commo()->rpc_par_proxies_[partition_id_]) {
      // TODO: don't send messages to self
      commo()->SendCommit(partition_id_, p.first, commit); 
    }
  }
  cb(); 
}

void PbftServer::OnCommit(const CommitMessage &mesg, const function<void()> &cb) {
  std::lock_guard<std::recursive_mutex> lock(mtx_); 
  bool is_same_view = mesg.view == current_view_; 
  bool is_seqno_valid = low_watermark_ < mesg.seqno && mesg.seqno <= high_watermark_; 
  bool is_digest_valid = true; // TODO: check signature of digest
  if (in_view_change_ || !is_same_view || !is_seqno_valid || !is_digest_valid || 
    !logstore_.HasPreprepare(mesg.view, mesg.seqno)) {
    return; 
  }

  Request &req = requests_[mesg.seqno]; 

  logstore_.AddCommit(mesg.view, mesg.seqno, mesg.server_id, mesg); 
  bool is_committed = logstore_.GetCommits(mesg.view, mesg.seqno).size() >= target_majority_;
  if (req.state == RequestState::REQ_PREPARED && is_committed) {
    req.state = RequestState::REQ_COMMITTED; 
    while (requests_.count(last_executed_ + 1)) {
      req = requests_[last_executed_ + 1]; 
      if (req.state == RequestState::REQ_COMMITTED) {
        replies_[std::make_pair(req.timestamp, req.client_id)] = app_next_(*req.cmd);
        req.state = RequestState::REQ_EXECUTED; 
        last_executed_++; 
        break;
      } else if (req.state == RequestState::REQ_EXECUTED) {
        last_executed_++; 
      } else {
        break; 
      }
    }
  }
  cb(); 
}

void PbftServer::OnCheckpoint(const CheckpointMessage &mesg, const function<void()> &cb) {
  std::lock_guard<std::recursive_mutex> lock(mtx_); 
  logstore_.AddCheckpoint(mesg.ckpt_seqno, mesg.server_id, mesg); 
  bool has_majority = logstore_.GetCheckpoints(mesg.ckpt_seqno).size() >= target_majority_; 
  if (has_majority && mesg.ckpt_seqno > low_watermark_) {
    low_watermark_ = mesg.ckpt_seqno; 
    high_watermark_ = low_watermark_ + MAX_REQUESTS_IN_TRANSIT;
    logstore_.ClearLog(current_view_, low_watermark_); 
    requests_.erase(requests_.begin(), requests_.upper_bound(low_watermark_)); 
  }
  cb();  
}

void PbftServer::OnViewChange(const ViewChangeMessage &mesg, const function<void()> &cb) {
  std::lock_guard<std::recursive_mutex> lock(mtx_); 
  in_view_change_ = true; 
  if (!logstore_.AddViewChange(mesg.new_view, mesg.server_id, mesg)) {
    return; 
  }

  bool has_majority = logstore_.GetViewChanges(mesg.new_view).size() >= target_majority_ - 1; 
  if ((current_primary_ == loc_id_) && has_majority) {
    if (!logstore_.HasViewChange(mesg.new_view, loc_id_)) {
      logstore_.AddViewChange(mesg.new_view, loc_id_, CreateViewChange(mesg.new_view)); 
    }
    NewViewMessage new_view = CreateNewView(mesg.new_view, logstore_.GetViewChanges(mesg.new_view)); 
    for (const auto &p : commo()->rpc_par_proxies_[partition_id_]) {
      commo()->SendNewView(partition_id_, p.first, new_view); 
    }
  }
  cb(); 
}

void PbftServer::OnNewView(const NewViewMessage &mesg, const function<void()> &cb) {
  std::lock_guard<std::recursive_mutex> lock(mtx_); 
  auto new_view = CreateNewView(mesg.new_view, mesg.view_changes);
  auto it = mesg.preprepares.begin(); 
  auto it2 = new_view.preprepares.begin(); 
  while (it != mesg.preprepares.end() && it2 != new_view.preprepares.end()) {
    if (it->first != it2->first ||
        it->second.view != it2->second.view ||
        it->second.seqno != it2->second.seqno ||
        it->second.digest != it2->second.digest ||
        it->second.server_id != it2->second.server_id) {
      return; 
    }
    it++; 
    it2++; 
  }
  
  current_view_ = mesg.new_view;
  current_primary_ = (current_view_ % num_proxies_); 
  logstore_.ClearLog(current_view_, low_watermark_); 
  for (const auto &[slot, preprepare] : mesg.preprepares) {
    if (logstore_.AddPreprepare(current_view_, slot, preprepare)) {
      if (!requests_.count(slot)) {
        auto cmptr = std::make_shared<TpcNoopCommand>(); 
        auto cmd = std::dynamic_pointer_cast<Marshallable>(cmptr);
        requests_[slot] = {cmd, 0, (cliid_t)num_proxies_, REQ_INIT};
      }
      PrepareMessage prepare = CreatePrepare(preprepare.view, preprepare.seqno);
      for (const auto &p : commo()->rpc_par_proxies_[partition_id_]) {
        commo()->SendPrepare(partition_id_, p.first, prepare); 
      }
    }
  }
  in_view_change_ = false; 
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
