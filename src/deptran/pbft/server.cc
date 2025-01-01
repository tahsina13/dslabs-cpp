

#include "server.h"
// #include "paxos_worker.h"
#include "exec.h"
#include "frame.h"
#include "coordinator.h"
#include "../classic/tpc_command.h"


namespace janus {

PbftServer::PbftServer(Frame * frame) : current_view_(0), low_watermark_(0), high_watermark_(0) {
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
}

void PbftServer::GetState(bool *is_primary, uint64_t *view) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  *is_primary = (current_view_ % num_proxies_ == loc_id_); 
  *view = current_view_; 
}

void PbftServer::OnPreprepare(const PreprepareRequest &req, 
                              const Message &mesg,
                              const function<void()> &cb) {
  // TODO: implement
  cb(); 
}

void PbftServer::OnPrepare(const PrepareRequest &req, const function<void()> &cb) {
  // TODO: implement
  cb(); 
}

void PbftServer::OnCommit(const CommitRequest &req, const function<void()> &cb) {
  // TODO: implement
  cb(); 
}

void PbftServer::OnPrepared(const PreparedRequest &req, const function<void()> &cb) {
  // TODO: implement
  cb();
}

void PbftServer::OnCommitted(const CommittedRequest &req, const function<void()> &cb) {
  // TODO: implement
  cb();  
}

void PbftServer::OnCheckpoint(const CheckpointRequest &req, const function<void()> &cb) {
  // TODO: implement
  cb();  
}

void PbftServer::OnNewView(const NewViewRequest &req, const function<void()> &cb) {
  // TODO: implement
  cb(); 
}

void PbftServer::OnViewChange(const ViewChangeRequest &req, const function<void()> &cb) {
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
