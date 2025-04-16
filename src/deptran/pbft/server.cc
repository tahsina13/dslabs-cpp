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
                                        md_(EVP_sha3_512()) {
  frame_ = frame ;
  /* Your code here for server initialization. Note that this function is 
     called in a different OS thread. Be careful about thread safety if 
     you want to initialize variables here. */

  verify((mdctx_ = EVP_MD_CTX_new()) != NULL);
  verify((privkey_ctx_ = EVP_PKEY_CTX_new(frame_->site_info_->privkey.get(), NULL)) != NULL);
}

PbftServer::~PbftServer() {
  /* Your code here for server teardown */

  EVP_MD_CTX_free(mdctx_); 
  EVP_PKEY_CTX_free(privkey_ctx_);
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
  target_ckpt_majority_ = faults + 1; // for pBFT-PK model

  Config *config = Config::GetConfig(); 
  for (const auto &p : commo()->rpc_par_proxies_[partition_id_]) {
    const auto &pubkey = config->SitePubKeyById(p.first); 
    if (pubkey != nullptr) {
      EVP_PKEY_CTX *pubkey_ctx = EVP_PKEY_CTX_new(pubkey.get(), NULL);
      std::shared_ptr<EVP_PKEY_CTX> pubkey_ctx_ptr(pubkey_ctx, [](EVP_PKEY_CTX *p) {
        EVP_PKEY_CTX_free(p); 
      });
      pubkey_ctx_[p.first] = pubkey_ctx_ptr; 
    } else {
      pubkey_ctx_[p.first] = nullptr; 
    } 
  }
}

bool PbftServer::Start(shared_ptr<Marshallable>& cmd, 
                       uint64_t timestamp, 
                       cliid_t client_id, 
                       uint64_t *index, 
                       uint64_t *view) {
  std::lock_guard<std::recursive_mutex> lock(mtx_); 
  // TODO: check client signature
  if (current_primary_ != site_id_) {
    return false; 
  }

  slotid_t seqno = low_watermark_ + 1; 
  while (seqno <= high_watermark_ && logstore_.HasPreprepare(seqno)) {
    seqno++; 
  }
  if (seqno > high_watermark_) {
    return false; 
  }

  std::vector<uint8_t> digest; // TODO: generate digest from cmd

  PreprepareMessage preprepare = CreatePreprepare(current_view_, seqno, digest); 
  SignPreprepare(preprepare); 
  if (!logstore_.AddPreprepare(seqno, preprepare)) {
    return false; 
  }
  for (const auto& p : commo()->rpc_par_proxies_[partition_id_]) {
    if (p.first != loc_id_) {
      commo()->SendPreprepare(partition_id_, p.first, preprepare, cmd, timestamp, client_id); 
    }
  }

  requests_[seqno] = {
    .cmd = cmd,
    .timestamp = timestamp,
    .client_id = client_id,
    .state = RequestState::REQ_INIT,
  }; 

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

std::vector<uint8_t> PbftServer::SignHash(const std::vector<uint8_t> &hash) {
  std::lock_guard<std::recursive_mutex> lock(mtx_); 
  size_t siglen; 
  unsigned char *raw_sig; 
  if (EVP_PKEY_sign_init(privkey_ctx_) != 1) {
    Log_fatal("EVP_PKEY_sign_init failed");
  }
  if (EVP_PKEY_CTX_set_rsa_padding(privkey_ctx_, RSA_PKCS1_PADDING) != 1) {
    Log_fatal("EVP_PKEY_CTX_set_rsa_padding failed");
  }
  if (EVP_PKEY_sign(privkey_ctx_, NULL, &siglen, 
                    (const unsigned char *)hash.data(), hash.size()) != 1) {
    Log_fatal("EVP_PKEY_sign failed");
  }
  if ((raw_sig = (unsigned char *)OPENSSL_malloc(siglen)) == NULL) {
    Log_fatal("OPENSSL_malloc failed");
  }
  if (EVP_PKEY_sign(privkey_ctx_, raw_sig, &siglen, 
                    (const unsigned char *)hash.data(), hash.size()) != 1) {
    Log_fatal("EVP_PKEY_sign failed");
  }
  std::vector<uint8_t> signature (raw_sig, raw_sig + siglen);
  OPENSSL_free(raw_sig); 
  return signature; 
}

bool PbftServer::VerifyHash(const std::vector<uint8_t> &hash, const std::vector<uint8_t> &signature, siteid_t site_id) {
  std::lock_guard<std::recursive_mutex> lock(mtx_); 
  verify(pubkey_ctx_.count(site_id) > 0); 
  EVP_PKEY_CTX *pubkey_ctx = pubkey_ctx_.at(site_id).get(); 
  if (pubkey_ctx == nullptr) {
    return false; 
  }
  if (EVP_PKEY_verify_init(pubkey_ctx) != 1) {
    Log_fatal("EVP_PKEY_verify_init failed");
  }
  if (EVP_PKEY_CTX_set_rsa_padding(pubkey_ctx, RSA_PKCS1_PADDING) != 1) {
    Log_fatal("EVP_PKEY_CTX_set_rsa_padding failed");
  }
  int ret = EVP_PKEY_verify(pubkey_ctx, (const unsigned char *)signature.data(), signature.size(), 
                            (const unsigned char *)hash.data(), hash.size());
  return ret == 1; 
}

PreprepareMessage PbftServer::CreatePreprepare(uint64_t view, slotid_t seqno, const std::vector<uint8_t> &digest) {
  return {
    .server_id = site_id_,
    .view = view,
    .seqno = seqno,
    .digest = digest,
  };
}

PrepareMessage PbftServer::CreatePrepare(uint64_t view, slotid_t seqno, const std::vector<uint8_t> &digest) {
  return {
    .server_id = site_id_,
    .view = view,
    .seqno = seqno,
    .digest = digest,
  }; 
}

CommitMessage PbftServer::CreateCommit(uint64_t view, slotid_t seqno, const std::vector<uint8_t> &digest) {
  return {
    .server_id = site_id_,
    .view = view,
    .seqno = seqno,
    .digest = digest,
  }; 
}

CheckpointMessage PbftServer::CreateCheckpoint(slotid_t ckpt_seqno, const std::vector<uint8_t> &ckpt_digest) {
  return {
    .server_id = site_id_,
    .ckpt_seqno = ckpt_seqno,
    .ckpt_digest = ckpt_digest,
  };
}

ViewChangeMessage PbftServer::CreateViewChange(uint64_t new_view) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  std::map<siteid_t, CheckpointMessage> checkpoints; 
  if (logstore_.GetCheckpointCount(low_watermark_) > 0) {
    checkpoints = logstore_.GetCheckpoints(low_watermark_);  
  }
  std::map<slotid_t, PreprepareMessage> preprepares; 
  std::map<slotid_t, std::map<siteid_t, PrepareMessage>> prepares; 
  for (const auto &request_entry : requests_) {
    const slotid_t slot = request_entry.first; 
    const Request &req = request_entry.second;  
    if (req.state >= RequestState::REQ_PREPARED) {
      preprepares[slot] = logstore_.GetPreprepare(slot);  
      prepares[slot] = logstore_.GetPrepares(slot); 
    }
  }  
  return {
    .server_id = site_id_,
    .new_view = new_view,
    .ckpt_seqno = low_watermark_,
    .checkpoints = checkpoints,
    .preprepares = preprepares,
    .prepares = prepares,
  }; 
}

NewViewMessage PbftServer::CreateNewView(uint64_t new_view, const std::map<siteid_t, ViewChangeMessage> &view_changes) {
  std::lock_guard<std::recursive_mutex> lock(mtx_); 
  slotid_t min_s = std::numeric_limits<slotid_t>::max(), max_s = 0;
  std::map<slotid_t, PreprepareMessage> preprepares; 
  for (const auto &view_change_entry : view_changes) {
    const ViewChangeMessage &view_change = view_change_entry.second; 
    bool is_ckpt_stable = (view_change.ckpt_seqno == 0) || // ckpt 0 stable by default 
      (view_change.checkpoints.size() >= target_ckpt_majority_);
    if (is_ckpt_stable) {
      min_s = std::min(min_s, view_change.ckpt_seqno);
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
      std::vector<uint8_t> digest; // TODO: generate digest from noop cmd
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

std::vector<uint8_t> PbftServer::GetPreprepareHash(const PreprepareMessage &mesg) {
  std::lock_guard<std::recursive_mutex> lock(mtx_); 
  unsigned char *raw_hash; 
  if (EVP_DigestInit_ex(mdctx_, md_, NULL) != 1) {
    Log_fatal("EVP_DigestInit_ex failed");
  }
  if (EVP_DigestUpdate(mdctx_, &mesg.server_id, sizeof(mesg.server_id)) != 1 ||
      EVP_DigestUpdate(mdctx_, &mesg.view, sizeof(mesg.view)) != 1 ||
      EVP_DigestUpdate(mdctx_, &mesg.seqno, sizeof(mesg.seqno)) != 1) {
    Log_fatal("EVP_DigestUpdate failed");
  }
  if ((raw_hash = (unsigned char *)OPENSSL_malloc(EVP_MD_size(md_))) == NULL) {
    Log_fatal("OPENSSL_malloc failed");
  }
  if (EVP_DigestFinal_ex(mdctx_, raw_hash, NULL) != 1) {
    Log_fatal("EVP_DigestFinal_ex failed");
  }
  std::vector<uint8_t> hash (raw_hash, raw_hash + EVP_MD_size(md_)); 
  OPENSSL_free(raw_hash);
  return hash; 
}

std::vector<uint8_t> PbftServer::GetPrepareHash(const PrepareMessage &mesg) {
  std::lock_guard<std::recursive_mutex> lock(mtx_); 
  unsigned char *raw_hash; 
  if (EVP_DigestInit_ex(mdctx_, md_, NULL) != 1) {
    Log_fatal("EVP_DigestInit_ex failed");
  }
  if (EVP_DigestUpdate(mdctx_, &mesg.server_id, sizeof(mesg.server_id)) != 1 ||
      EVP_DigestUpdate(mdctx_, &mesg.view, sizeof(mesg.view)) != 1 ||
      EVP_DigestUpdate(mdctx_, &mesg.seqno, sizeof(mesg.seqno)) != 1 ||
      EVP_DigestUpdate(mdctx_, mesg.digest.data(), mesg.digest.size()) != 1) {
    Log_fatal("EVP_DigestUpdate failed");
  }
  if ((raw_hash = (unsigned char *)OPENSSL_malloc(EVP_MD_size(md_))) == NULL) {
    Log_fatal("OPENSSL_malloc failed");
  }
  if (EVP_DigestFinal_ex(mdctx_, raw_hash, NULL) != 1) {
    Log_fatal("EVP_DigestFinal_ex failed");
  }
  std::vector<uint8_t> hash (raw_hash, raw_hash + EVP_MD_size(md_));
  OPENSSL_free(raw_hash);
  return hash; 
}

std::vector<uint8_t> PbftServer::GetCommitHash(const CommitMessage &mesg) {
  std::lock_guard<std::recursive_mutex> lock(mtx_); 
  unsigned char *raw_hash; 
  if (EVP_DigestInit_ex(mdctx_, md_, NULL) != 1) {
    Log_fatal("EVP_DigestInit_ex failed");
  }
  if (EVP_DigestUpdate(mdctx_, &mesg.server_id, sizeof(mesg.server_id)) != 1 ||
      EVP_DigestUpdate(mdctx_, &mesg.view, sizeof(mesg.view)) != 1 ||
      EVP_DigestUpdate(mdctx_, &mesg.seqno, sizeof(mesg.seqno)) != 1 ||
      EVP_DigestUpdate(mdctx_, mesg.digest.data(), mesg.digest.size()) != 1) {
    Log_fatal("EVP_DigestUpdate failed");
  }
  if ((raw_hash = (unsigned char *)OPENSSL_malloc(EVP_MD_size(md_))) == NULL) {
    Log_fatal("OPENSSL_malloc failed");
  }
  if (EVP_DigestFinal_ex(mdctx_, raw_hash, NULL) != 1) {
    Log_fatal("EVP_DigestFinal_ex failed");
  }
  std::vector<uint8_t> hash (raw_hash, raw_hash + EVP_MD_size(md_));
  OPENSSL_free(raw_hash);
  return hash; 
}

std::vector<uint8_t> PbftServer::GetCheckpointHash(const CheckpointMessage &mesg) {
  std::lock_guard<std::recursive_mutex> lock(mtx_); 
  unsigned char *raw_hash; 
  if (EVP_DigestInit_ex(mdctx_, md_, NULL) != 1) {
    Log_fatal("EVP_DigestInit_ex failed");
  }
  if (EVP_DigestUpdate(mdctx_, &mesg.server_id, sizeof(mesg.server_id)) != 1 ||
      EVP_DigestUpdate(mdctx_, &mesg.ckpt_seqno, sizeof(mesg.ckpt_seqno)) != 1 ||
      EVP_DigestUpdate(mdctx_, mesg.ckpt_digest.data(), mesg.ckpt_digest.size()) != 1) {
    Log_fatal("EVP_DigestUpdate failed");
  }
  if ((raw_hash = (unsigned char *)OPENSSL_malloc(EVP_MD_size(md_))) == NULL) {
    Log_fatal("OPENSSL_malloc failed");
  }
  if (EVP_DigestFinal_ex(mdctx_, raw_hash, NULL) != 1) {
    Log_fatal("EVP_DigestFinal_ex failed");
  }
  std::vector<uint8_t> hash (raw_hash, raw_hash + EVP_MD_size(md_));
  OPENSSL_free(raw_hash);
  return hash; 
}

std::vector<uint8_t> PbftServer::GetViewChangeHash(const ViewChangeMessage &mesg) {
  std::lock_guard<std::recursive_mutex> lock(mtx_); 
  unsigned char *raw_hash; 
  if (EVP_DigestInit_ex(mdctx_, md_, NULL) != 1) {
    Log_fatal("EVP_DigestInit_ex failed");
  } 
  if (EVP_DigestUpdate(mdctx_, &mesg.server_id, sizeof(mesg.server_id)) != 1 ||
      EVP_DigestUpdate(mdctx_, &mesg.new_view, sizeof(mesg.new_view)) != 1 ||
      EVP_DigestUpdate(mdctx_, &mesg.ckpt_seqno, sizeof(mesg.ckpt_seqno)) != 1) {
    Log_fatal("EVP_DigestUpdate failed");
  }
  for (const auto &ckpt_entry : mesg.checkpoints) {
    const CheckpointMessage &ckpt = ckpt_entry.second; 
    if (EVP_DigestUpdate(mdctx_, ckpt.ckpt_digest.data(), ckpt.ckpt_digest.size()) != 1) {
      Log_fatal("EVP_DigestUpdate failed");
    } 
  }
  for (const auto &preprepare_entry : mesg.preprepares) {
    const PreprepareMessage &preprepare = preprepare_entry.second; 
    if (EVP_DigestUpdate(mdctx_, preprepare.digest.data(), preprepare.digest.size()) != 1) {
      Log_fatal("EVP_DigestUpdate failed");
    } 
  }
  for (const auto &prepare_map : mesg.prepares) {
    for (const auto &prepare_entry : prepare_map.second) {
      const PrepareMessage &prepare = prepare_entry.second; 
      if (EVP_DigestUpdate(mdctx_, prepare.digest.data(), prepare.digest.size()) != 1) {
        Log_fatal("EVP_DigestUpdate failed");
      } 
    }
  }
  if ((raw_hash = (unsigned char *)OPENSSL_malloc(EVP_MD_size(md_))) == NULL) {
    Log_fatal("OPENSSL_malloc failed");
  }
  if (EVP_DigestFinal_ex(mdctx_, raw_hash, NULL) != 1) {
    Log_fatal("EVP_DigestFinal_ex failed");
  }
  std::vector<uint8_t> hash (raw_hash, raw_hash + EVP_MD_size(md_));
  OPENSSL_free(raw_hash);
  return hash; 
}

std::vector<uint8_t> PbftServer::GetNewViewHash(const NewViewMessage &mesg) {
  std::lock_guard<std::recursive_mutex> lock(mtx_); 
  unsigned char *raw_hash; 
  if (EVP_DigestInit_ex(mdctx_, md_, NULL) != 1) {
    Log_fatal("EVP_DigestInit_ex failed");
  } 
  if (EVP_DigestUpdate(mdctx_, &mesg.server_id, sizeof(mesg.server_id)) != 1 ||
      EVP_DigestUpdate(mdctx_, &mesg.new_view, sizeof(mesg.new_view)) != 1) {
    Log_fatal("EVP_DigestUpdate failed");
  }
  for (const auto &view_change_entry : mesg.view_changes) {
    const ViewChangeMessage &view_change = view_change_entry.second; 
    if (EVP_DigestUpdate(mdctx_, view_change.signature.data(), view_change.signature.size()) != 1) {
      Log_fatal("EVP_DigestUpdate failed");
    } 
  }
  for (const auto &preprepare_entry : mesg.preprepares) {
    const PreprepareMessage &preprepare = preprepare_entry.second; 
    if (EVP_DigestUpdate(mdctx_, preprepare.digest.data(), preprepare.digest.size()) != 1) {
      Log_fatal("EVP_DigestUpdate failed");
    } 
  }
  if ((raw_hash = (unsigned char *)OPENSSL_malloc(EVP_MD_size(md_))) == NULL) {
    Log_fatal("OPENSSL_malloc failed");
  }
  if (EVP_DigestFinal_ex(mdctx_, raw_hash, NULL) != 1) {
    Log_fatal("EVP_DigestFinal_ex failed");
  }
  std::vector<uint8_t> hash (raw_hash, raw_hash + EVP_MD_size(md_));
  OPENSSL_free(raw_hash);
  return hash; 
}

void PbftServer::SignPreprepare(PreprepareMessage &mesg) {
  std::vector<uint8_t> hash = GetPreprepareHash(mesg); 
  mesg.signature = SignHash(hash);
}

void PbftServer::SignPrepare(PrepareMessage &mesg) {
  std::vector<uint8_t> hash = GetPrepareHash(mesg); 
  mesg.signature = SignHash(hash);
}

void PbftServer::SignCommit(CommitMessage &mesg) {
  std::vector<uint8_t> hash = GetCommitHash(mesg); 
  mesg.signature = SignHash(hash);
}

void PbftServer::SignCheckpoint(CheckpointMessage &mesg) {
  std::vector<uint8_t> hash = GetCheckpointHash(mesg); 
  mesg.signature = SignHash(hash);
}

void PbftServer::SignViewChange(ViewChangeMessage &mesg) {
  std::vector<uint8_t> hash = GetViewChangeHash(mesg); 
  mesg.signature = SignHash(hash);
}

void PbftServer::SignNewView(NewViewMessage &mesg) {
  std::vector<uint8_t> hash = GetNewViewHash(mesg); 
  mesg.signature = SignHash(hash);
}

bool PbftServer::VerifyPreprepare(const PreprepareMessage &mesg) {
  std::vector<uint8_t> hash = GetPreprepareHash(mesg); 
  return VerifyHash(hash, mesg.signature, mesg.server_id); 
}

bool PbftServer::VerifyPrepare(const PrepareMessage &mesg) {
  std::vector<uint8_t> hash = GetPrepareHash(mesg); 
  return VerifyHash(hash, mesg.signature, mesg.server_id); 
}

bool PbftServer::VerifyCommit(const CommitMessage &mesg) {
  std::vector<uint8_t> hash = GetCommitHash(mesg); 
  return VerifyHash(hash, mesg.signature, mesg.server_id); 
}

bool PbftServer::VerifyCheckpoint(const CheckpointMessage &mesg) {
  std::vector<uint8_t> hash = GetCheckpointHash(mesg); 
  return VerifyHash(hash, mesg.signature, mesg.server_id); 
}

bool PbftServer::VerifyViewChange(const ViewChangeMessage &mesg) {
  for (const auto &ckpt_entry : mesg.checkpoints) {
    const CheckpointMessage &ckpt = ckpt_entry.second;
    if (!VerifyCheckpoint(ckpt)) {
      return false; 
    }
  }
  for (const auto &preprepare_entry : mesg.preprepares) {
    const PreprepareMessage &preprepare = preprepare_entry.second;
    if (!VerifyPreprepare(preprepare)) {
      return false; 
    }
  }
  for (const auto &prepare_map : mesg.prepares) {
    for (const auto &prepare_entry : prepare_map.second) {
      const PrepareMessage &prepare = prepare_entry.second;
      if (!VerifyPrepare(prepare)) {
        return false; 
      }
    }
  }
  std::vector<uint8_t> hash = GetViewChangeHash(mesg);
  return VerifyHash(hash, mesg.signature, mesg.server_id);
}

bool PbftServer::VerifyNewView(const NewViewMessage &mesg) {
  for (const auto &view_change_entry : mesg.view_changes) {
    const ViewChangeMessage &view_change = view_change_entry.second; 
    if (!VerifyViewChange(view_change)) {
      return false; 
    }
  }
  std::vector<uint8_t> hash = GetNewViewHash(mesg); 
  return VerifyHash(hash, mesg.signature, mesg.server_id); 
}

void PbftServer::HandlePreprepare(const PreprepareMessage &mesg) {
  std::lock_guard<std::recursive_mutex> lock(mtx_); 
  // Assumes preprepare message is valid and correct
  logstore_.AddPreprepare(mesg.seqno, mesg);  
  PrepareMessage prepare = CreatePrepare(current_view_, mesg.seqno, mesg.digest); 
  SignPrepare(prepare);
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
    Request &req = requests_.at(mesg.seqno); 
    req.state = RequestState::REQ_PREPARED; 
    CommitMessage commit = CreateCommit(current_view_, mesg.seqno, mesg.digest); 
    SignCommit(commit); 
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
    Request &req = requests_.at(mesg.seqno);
    req.state = RequestState::REQ_COMMITTED; 
    while (requests_.count(last_executed_ + 1)) {
      req = requests_.at(last_executed_ + 1); 
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
}

void PbftServer::HandleCheckpoint(const CheckpointMessage &mesg) {
  std::lock_guard<std::recursive_mutex> lock(mtx_); 
  logstore_.AddCheckpoint(mesg.ckpt_seqno, mesg.server_id, mesg); 
  bool has_majority = logstore_.GetCheckpointCount(mesg.ckpt_seqno) >= target_ckpt_majority_; 
  if (has_majority && mesg.ckpt_seqno > low_watermark_) {
    low_watermark_ = mesg.ckpt_seqno; 
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
    if (!logstore_.HasViewChange(mesg.new_view, loc_id_)) {
      logstore_.AddViewChange(mesg.new_view, loc_id_, CreateViewChange(mesg.new_view)); 
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
                              std::shared_ptr<Marshallable> cmd,
                              uint64_t timestamp,
                              cliid_t client_id,
                              const function<void()> &cb) {
  std::lock_guard<std::recursive_mutex> lock(mtx_); 
  if (in_view_change_ || !VerifyPreprepare(mesg)) {
    return; 
  }

  if (logstore_.HasPreprepare(mesg.seqno) || requests_.count(mesg.seqno) != 0) {
    return; 
  }

  std::vector<uint8_t> digest = mesg.digest; // TODO: figure out how to generate digest from cmd

  bool is_from_primary = mesg.server_id == current_primary_;
  bool is_same_view = mesg.view == current_view_; 
  bool is_seqno_valid = low_watermark_ < mesg.seqno && mesg.seqno <= high_watermark_; 
  bool is_digest_valid = mesg.digest == digest; 
  if (!is_from_primary || !is_same_view || !is_seqno_valid || !is_digest_valid) {
    return; 
  }

  HandlePreprepare(mesg); 
  requests_[mesg.seqno] = {
    .cmd = cmd,
    .timestamp = timestamp,
    .client_id = client_id,
    .state = RequestState::REQ_INIT,
  }; 
  cb(); 
}

void PbftServer::OnPrepare(const PrepareMessage &mesg, const function<void()> &cb) {
  std::lock_guard<std::recursive_mutex> lock(mtx_); 
  if (in_view_change_ || !VerifyPrepare(mesg)) {
    return; 
  }

  if (!logstore_.HasPreprepare(mesg.seqno) || requests_.count(mesg.seqno) == 0) {
    return; 
  }
  const PreprepareMessage &preprepare = logstore_.GetPreprepare(mesg.seqno);
  const Request &req = requests_.at(mesg.seqno);

  bool is_from_primary = mesg.server_id == current_primary_; 
  bool is_same_view = mesg.view == current_view_; 
  bool is_seqno_valid = low_watermark_ < mesg.seqno && mesg.seqno <= high_watermark_; 
  bool is_matches = preprepare.view == mesg.view && 
                    preprepare.seqno == mesg.seqno && 
                    preprepare.digest == mesg.digest;
  bool is_req_init = req.state == RequestState::REQ_INIT;
  if (is_from_primary || !is_same_view || !is_seqno_valid || !is_matches || !is_req_init) {
    return; 
  }

  HandlePrepare(mesg); 
  cb(); 
}

void PbftServer::OnCommit(const CommitMessage &mesg, const function<void()> &cb) {
  std::lock_guard<std::recursive_mutex> lock(mtx_); 
  if (in_view_change_ || !VerifyCommit(mesg)) {
    return; 
  }

  if (!logstore_.HasPreprepare(mesg.seqno) || requests_.count(mesg.seqno) == 0) {
    return; 
  }
  const PreprepareMessage &preprepare = logstore_.GetPreprepare(mesg.seqno);
  const Request &req = requests_.at(mesg.seqno);

  bool is_same_view = mesg.view == current_view_; 
  bool is_seqno_valid = low_watermark_ < mesg.seqno && mesg.seqno <= high_watermark_; 
  bool is_matches = preprepare.view == mesg.view && 
                    preprepare.seqno == mesg.seqno && 
                    preprepare.digest == mesg.digest;
  bool is_req_prepared = req.state == RequestState::REQ_PREPARED;
  if (in_view_change_ || !is_same_view || !is_seqno_valid || !is_matches || !is_req_prepared) {
    return; 
  }

  HandleCommit(mesg);
  cb(); 
}

void PbftServer::OnCheckpoint(const CheckpointMessage &mesg, const function<void()> &cb) {
  std::lock_guard<std::recursive_mutex> lock(mtx_); 
  if (!VerifyCheckpoint(mesg)) {
    return; 
  }
  HandleCheckpoint(mesg);
  cb();  
}

void PbftServer::OnViewChange(const ViewChangeMessage &mesg, const function<void()> &cb) {
  std::lock_guard<std::recursive_mutex> lock(mtx_); 
  if (!VerifyViewChange(mesg)) {
    return; 
  }
  HandleViewChange(mesg); 
  cb(); 
}

void PbftServer::OnNewView(const NewViewMessage &mesg, const function<void()> &cb) {
  std::lock_guard<std::recursive_mutex> lock(mtx_); 
  if (!VerifyNewView(mesg)) {
    return; 
  }
  HandleNewView(mesg); 
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
