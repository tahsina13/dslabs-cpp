#include "testconf.h"
#include "marshallable.h"

namespace janus {

#ifdef PBFT_TEST_CORO

int _test_id_g = 0;

PbftFrame **PbftTestConfig::replicas = nullptr;
std::function<std::string(Marshallable &)> PbftTestConfig::commit_callbacks[NSERVERS];
std::vector<int> PbftTestConfig::committed_cmds[NSERVERS];
uint64_t PbftTestConfig::rpc_count_last[NSERVERS];

PbftTestConfig::PbftTestConfig(PbftFrame **replicas) : md_(EVP_sha512()) {
  verify(PbftTestConfig::replicas == nullptr);
  PbftTestConfig::replicas = replicas;
  for (int i = 0; i < NSERVERS; i++) {
    PbftTestConfig::replicas[i]->svr_->rep_frame_ = PbftTestConfig::replicas[i]->svr_->frame_;
    PbftTestConfig::committed_cmds[i].push_back(-1);
    PbftTestConfig::rpc_count_last[i] = 0;
    disconnected_[i] = false;
  }
  for (const auto &p : Config::GetConfig()->GetMyClients()) {
    if (p.privkey != nullptr) {
      privkey_auth_.emplace(std::piecewise_construct,
                            std::forward_as_tuple(p.id),
                            std::forward_as_tuple(md_, p.privkey));
    }
  }
  th_ = std::thread([this](){ netctlLoop(); });
}

void PbftTestConfig::SetLearnerAction(void) {
  for (int i = 0; i < NSERVERS; i++) {
    PbftTestConfig::commit_callbacks[i] = [i](Marshallable& cmd) -> std::string {
      verify(cmd.kind_ == MarshallDeputy::CMD_TPC_COMMIT);
      auto& command = dynamic_cast<TpcCommitCommand&>(cmd);
      Log_debug("server %d committed value %d", i, command.tx_id_);
      PbftTestConfig::committed_cmds[i].push_back(command.tx_id_);
      return std::to_string(command.tx_id_); // just return transaction id for now
    };
    PbftTestConfig::replicas[i]->svr_->RegLearnerAction(PbftTestConfig::commit_callbacks[i]);
  }
}

bool PbftTestConfig::ViewMovedOn(uint64_t view) {
  for (int i = 0; i < NSERVERS; i++) {
    uint64_t curView;
    bool isPrimary;
    PbftTestConfig::replicas[i]->svr_->GetState(&isPrimary, &curView);
    if (curView > view) {
      return true;
    }
  }
  return false;
}

uint64_t PbftTestConfig::OneView(void) {
  uint64_t view, curView;
  bool isPrimary;
  PbftTestConfig::replicas[0]->svr_->GetState(&isPrimary, &view);
  for (int i = 1; i < NSERVERS; i++) {
    PbftTestConfig::replicas[i]->svr_->GetState(&isPrimary, &curView);
    if (curView != view) {
      return -1;
    }
  }
  return view; 
}

int PbftTestConfig::NCommitted(uint64_t index) {
  int cmd, n = 0;
  for (int i = 0; i < NSERVERS; i++) {
    if (PbftTestConfig::committed_cmds[i].size() > index) {
      auto curcmd = PbftTestConfig::committed_cmds[i][index];
      if (n == 0) {
        cmd = curcmd;
      } else {
        if (curcmd != cmd) {
          return -1;
        }
      }
      n++;
    }
  }
  return n;
}

bool PbftTestConfig::Start(int svr, int cmd, cliid_t client_id, uint64_t *index, uint64_t *view) {
  // Construct an empty TpcCommitCommand containing cmd as its tx_id_
  auto cmdptr = std::make_shared<TpcCommitCommand>();
  auto vpd_p = std::make_shared<VecPieceData>();
  vpd_p->sp_vec_piece_data_ = std::make_shared<vector<shared_ptr<SimpleCommand>>>();
  cmdptr->tx_id_ = cmd;
  cmdptr->cmd_ = vpd_p;
  auto cmdptr_m = dynamic_pointer_cast<Marshallable>(cmdptr);
  auto now = chrono::steady_clock::now(); 
  uint64_t timestamp = chrono::duration_cast<chrono::seconds>(now.time_since_epoch()).count(); 
  // call Start()
  Log_debug("Starting agreement on svr %d for cmd id %d", svr, cmdptr->tx_id_);
  Request req {
    .timestamp = timestamp,
    .client_id = client_id,
  };
  privkey_auth_.at(client_id).SignRequest(cmdptr_m, req); 
  return PbftTestConfig::replicas[svr]->svr_->Start(cmdptr_m, req, index, view);
}

int PbftTestConfig::Wait(uint64_t index, int n, uint64_t view) {
  int nc = 0, i;
  auto to = 10000; // 10 milliseconds
  for (i = 0; i < 30; i++) {
    nc = NCommitted(index);
    if (nc < 0) {
      return -3; // values differ
    } else if (nc >= n) {
      break;
    }
    Reactor::CreateSpEvent<TimeoutEvent>(to)->Wait();
    if (to < 1000000) {
      to *= 2;
    }
    if (ViewMovedOn(view)) {
      return -2; // term changed
    }
  }
  if (i == 30) {
    return -1; // timeout
  }
  for (int i = 0; i < NSERVERS; i++) {
    if (PbftTestConfig::committed_cmds[i].size() > index) {
      return PbftTestConfig::committed_cmds[i][index];
    }
  }
  verify(0);
}

uint64_t PbftTestConfig::DoAgreement(int cmd, cliid_t client_id, int n, bool retry) {
  Log_debug("Doing 1 round of Pbft agreement");
  auto start = chrono::steady_clock::now();
  while ((chrono::steady_clock::now() - start) < chrono::seconds{10}) {
    usleep(50000);
    // Coroutine::Sleep(50000);
    // Call Start() to all servers until leader is found
    int ldr = -1;
    uint64_t index, view;
    for (int i = 0; i < NSERVERS; i++) {
      // skip disconnected servers
      if (PbftTestConfig::replicas[i]->svr_->IsDisconnected())
        continue;
      if (Start(i, cmd, client_id, &index, &view)) {
        Log_debug("starting cmd ldr=%d cmd=%d index=%ld view=%ld", 
            PbftTestConfig::replicas[i]->svr_->loc_id_, cmd, index, view);
        ldr = i;
        break;
      }
    }
    if (ldr != -1) {
      // If Start() successfully called, wait for agreement
      auto start2 = chrono::steady_clock::now();
      int nc;
      while ((chrono::steady_clock::now() - start2) < chrono::seconds{2}) {
        nc = NCommitted(index);
        if (nc < 0) {
          break;
        } else if (nc >= n) {
          for (int i = 0; i < NSERVERS; i++) {
            if (PbftTestConfig::committed_cmds[i].size() > index) {
              Log_debug("found commit log");
              auto cmd2 = PbftTestConfig::committed_cmds[i][index];
              if (cmd == cmd2) {
                return index;
              }
              break;
            }
          }
          break;
        }
        usleep(20000);
        // Coroutine::Sleep(50000);
      }
      Log_debug("%d committed server at index %d", nc, index);
      if (!retry) {
          Log_debug("failed to reach agreement");
          return 0;
        }
    } else {
      // If no leader found, sleep and retry.
      usleep(50000);
      // Coroutine::Sleep(50000);
    }
  }
  Log_debug("Failed to reach agreement end");
  return 0;
}

void PbftTestConfig::Disconnect(int svr) {
  verify(svr >= 0 && svr < NSERVERS);
  std::lock_guard<std::mutex> lk(disconnect_mtx_);
  verify(!disconnected_[svr]);
  disconnect(svr, true);
  disconnected_[svr] = true;
}

void PbftTestConfig::Reconnect(int svr) {
  verify(svr >= 0 && svr < NSERVERS);
  std::lock_guard<std::mutex> lk(disconnect_mtx_);
  verify(disconnected_[svr]);
  reconnect(svr);
  disconnected_[svr] = false;
}

int PbftTestConfig::NDisconnected(void) {
  int count = 0;
  for (int i = 0; i < NSERVERS; i++) {
    if (disconnected_[i])
      count++;
  }
  return count;
}

void PbftTestConfig::SetUnreliable(bool unreliable) {
  std::unique_lock<std::mutex> lk(cv_m_);
  verify(!finished_);
  if (unreliable) {
    verify(!unreliable_);
    // lk acquired cv_m_ in state 1 or 0
    unreliable_ = true;
    // if cv_m_ was in state 1, must signal cv_ to wake up netctlLoop
    lk.unlock();
    cv_.notify_one();
  } else {
    verify(unreliable_);
    // lk acquired cv_m_ in state 2 or 0
    unreliable_ = false;
    // wait until netctlLoop moves cv_m_ from state 2 (or 0) to state 1,
    // restoring the network to reliable state in the process.
    lk.unlock();
    lk.lock();
  }
}

bool PbftTestConfig::IsUnreliable(void) {
  return unreliable_;
}

void PbftTestConfig::Shutdown(void) {
  // trigger netctlLoop shutdown
  {
    std::unique_lock<std::mutex> lk(cv_m_);
    verify(!finished_);
    // lk acquired cv_m_ in state 0, 1, or 2
    finished_ = true;
    // if cv_m_ was in state 1, must signal cv_ to wake up netctlLoop
    lk.unlock();
    cv_.notify_one();
  }
  // wait for netctlLoop thread to exit
  th_.join();
  // Reconnect() all Deconnect()ed servers
  for (int i = 0; i < NSERVERS; i++) {
    if (disconnected_[i]) {
      Reconnect(i);
    }
  }
}

uint64_t PbftTestConfig::RpcCount(int svr, bool reset) {
  std::lock_guard<std::recursive_mutex> lk(
    PbftTestConfig::replicas[svr]->commo_->rpc_mtx_);
  uint64_t count = PbftTestConfig::replicas[svr]->commo_->rpc_count_;
  uint64_t count_last = PbftTestConfig::rpc_count_last[svr];
  if (reset) {
    PbftTestConfig::rpc_count_last[svr] = count;
  }
  verify(count >= count_last);
  return count - count_last;
}

uint64_t PbftTestConfig::RpcTotal(void) {
  uint64_t total = 0;
  for (int i = 0; i < NSERVERS; i++) {
    total += PbftTestConfig::replicas[i]->commo_->rpc_count_;
  }
  return total;
}

bool PbftTestConfig::ServerCommitted(int svr, uint64_t index, int cmd) {
  if (PbftTestConfig::committed_cmds[svr].size() <= index)
    return false;
  return PbftTestConfig::committed_cmds[svr][index] == cmd;
}

void PbftTestConfig::netctlLoop(void) {
  int i;
  bool isdown;
  // cv_m_ unlocked state 0 (finished_ == false)
  std::unique_lock<std::mutex> lk(cv_m_);
  while (!finished_) {
    if (!unreliable_) {
      {
        std::lock_guard<std::mutex> prlk(disconnect_mtx_);
        // unset all unreliable-related disconnects and slows
        for (i = 0; i < NSERVERS; i++) {
          if (!disconnected_[i]) {
            reconnect(i, true);
            slow(i, 0);
          }
        }
      }
      // sleep until unreliable_ or finished_ is set
      // cv_m_ unlocked state 1 (unreliable_ == false && finished_ == false)
      cv_.wait(lk, [this](){ return unreliable_ || finished_; });
      continue;
    }
    {
      std::lock_guard<std::mutex> prlk(disconnect_mtx_);
      for (i = 0; i < NSERVERS; i++) {
        // skip server if it was disconnected using Disconnect()
        if (disconnected_[i]) {
          continue;
        }
        // server has DOWNRATE_N / DOWNRATE_D chance of being down
        if ((rand() % DOWNRATE_D) < DOWNRATE_N) {
          // disconnect server if not already disconnected in the previous period
          disconnect(i, true);
        } else {
          // Server not down: random slow timeout
          // Reconnect server if it was disconnected in the previous period
          reconnect(i, true);
          // server's slow timeout should be btwn 0-(MAXSLOW-1) ms
          slow(i, rand() % MAXSLOW);
        }
      }
    }
    // change unreliable state every 0.1s
    usleep(100000);
    // Coroutine::Sleep(100000);
    lk.unlock();
    // cv_m_ unlocked state 2 (unreliable_ == true && finished_ == false)
    lk.lock();
  }
  // If network is still unreliable, unset it
  if (unreliable_) {
    unreliable_ = false;
    {
      std::lock_guard<std::mutex> prlk(disconnect_mtx_);
      // unset all unreliable-related disconnects and slows
      for (i = 0; i < NSERVERS; i++) {
        if (!disconnected_[i]) {
          reconnect(i, true);
          slow(i, 0);
        }
      }
    }
  }
  // cv_m_ unlocked state 3 (unreliable_ == false && finished_ == true)
}

bool PbftTestConfig::isDisconnected(int svr) {
  std::lock_guard<std::recursive_mutex> lk(connection_m_);
  return PbftTestConfig::replicas[svr]->svr_->IsDisconnected();
}

void PbftTestConfig::disconnect(int svr, bool ignore) {
  std::lock_guard<std::recursive_mutex> lk(connection_m_);
  if (!isDisconnected(svr)) {
    // simulate disconnected server
    PbftTestConfig::replicas[svr]->svr_->Disconnect();
  } else if (!ignore) {
    verify(0);
  }
}

void PbftTestConfig::reconnect(int svr, bool ignore) {
  std::lock_guard<std::recursive_mutex> lk(connection_m_);
  if (isDisconnected(svr)) {
    // simulate reconnected server
    PbftTestConfig::replicas[svr]->svr_->Reconnect();
  } else if (!ignore) {
    verify(0);
  }
}

void PbftTestConfig::slow(int svr, uint32_t msec) {
  std::lock_guard<std::recursive_mutex> lk(connection_m_);
  verify(!isDisconnected(svr));
  // Ref<PollMgr> cpoll = borrow_const(PbftTestConfig::replicas[svr]->commo_->rpc_poll_);
  PbftTestConfig::replicas[svr]->commo_->rpc_poll_->slow(msec * 1000);
}

PbftServer *PbftTestConfig::GetServer(int svr) {
  return PbftTestConfig::replicas[svr]->svr_;
}

#endif

}
