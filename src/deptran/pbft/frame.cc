#include <openssl/bio.h>
#include <openssl/pem.h>

#include "../__dep__.h"
#include "../constants.h"
#include "frame.h"
#include "exec.h"
#include "coordinator.h"
#include "server.h"
#include "service.h"
#include "commo.h"
#include "config.h"
#include "test.h"

namespace janus {

REG_FRAME(MODE_FPGA_RAFT, vector<string>({"pbft"}), PbftFrame);

/*
template<typename D>
struct automatic_register {
 private:
  struct exec_register {
    exec_register() {
      D::do_it();
    }
  };
  // will force instantiation of definition of static member
  template<exec_register&> struct ref_it { };

  static exec_register register_object;
  static ref_it<register_object> referrer;
};

template<typename D> typename automatic_register<D>::exec_register
    automatic_register<D>::register_object;

struct foo : automatic_register<foo> {
  static void do_it() {
    REG_FRAME(MODE_FPGA_RAFT, vector<string>({"fpga_raft"}), PbftFrame);
  }
};*/

PbftFrame::PbftFrame(int mode) : Frame(mode) {

}

#ifdef PBFT_TEST_CORO
std::mutex PbftFrame::pbft_test_mutex_;
std::shared_ptr<Coroutine> PbftFrame::pbft_test_coro_ = nullptr;
uint16_t PbftFrame::n_replicas_ = 0;
PbftFrame *PbftFrame::replicas_[4];
uint16_t PbftFrame::n_commo_ = 0;
std::shared_ptr<EVP_PKEY> PbftFrame::privkey_ = nullptr;  
std::shared_ptr<EVP_PKEY> PbftFrame::pubkey_ = nullptr;
bool PbftFrame::tests_done_ = false;
#endif

Executor *PbftFrame::CreateExecutor(cmdid_t cmd_id, TxLogServer *sched) {
  Executor *exec = new PbftExecutor(cmd_id, sched);
  return exec;
}

Coordinator *PbftFrame::CreateCoordinator(cooid_t coo_id,
                                                Config *config,
                                                int benchmark,
                                                ClientControlServiceImpl *ccsi,
                                                uint32_t id,
                                                shared_ptr<TxnRegistry> txn_reg) {
  verify(config != nullptr);
  CoordinatorPbft *coo;
  coo = new CoordinatorPbft(coo_id,
                                  benchmark,
                                  ccsi,
                                  id);
  coo->frame_ = this;
  verify(commo_ != nullptr);
  coo->commo_ = commo_;
  /* TODO: remove when have a class for common data */
  verify(svr_ != nullptr);
  coo->svr_ = this->svr_;
  coo->slot_hint_ = &slot_hint_;
  coo->slot_id_ = slot_hint_++;
  coo->n_replica_ = config->GetPartitionSize(site_info_->partition_id_);
  coo->loc_id_ = this->site_info_->locale_id;
  verify(coo->n_replica_ != 0); // TODO
  Log_debug("create new fpga raft coord, coo_id: %d", (int) coo->coo_id_);
  return coo;
}

TxLogServer *PbftFrame::CreateScheduler() {
#ifdef PBFT_TEST_CORO
  pbft_test_mutex_.lock();
  if (privkey_ == nullptr) {
    EVP_PKEY *key = EVP_RSA_gen(2048);
    BIO *mem = BIO_new(BIO_s_mem());
    PEM_write_bio_PUBKEY(mem, key); 
    EVP_PKEY *pubkey = PEM_read_bio_PUBKEY(mem, NULL, NULL, NULL);
    BIO_free(mem); 
    privkey_ = std::shared_ptr<EVP_PKEY>(key, [](EVP_PKEY *p) {
      EVP_PKEY_free(p);
    });
    pubkey_ = std::shared_ptr<EVP_PKEY>(pubkey, [](EVP_PKEY *p) {
      EVP_PKEY_free(p);
    });
  }
  pbft_test_mutex_.unlock();
#endif

  if(svr_ == nullptr)
  {
    svr_ = new PbftServer(this); 
  }
  else
  {
    verify(0) ;
  }
  Log_debug("create new fpga raft sched loc: %d", this->site_info_->locale_id);

#ifdef PBFT_TEST_CORO
  pbft_test_mutex_.lock();
  verify(n_replicas_ < 4);
  replicas_[n_replicas_++] = this;
  pbft_test_mutex_.unlock();
#endif

  return svr_ ;
}

Communicator *PbftFrame::CreateCommo(PollMgr *poll) {
  // We only have 1 instance of PbftFrame object that is returned from
  // GetFrame method. PbftCommo currently seems ok to share among the
  // clients of this method.
  if (commo_ == nullptr) {
    commo_ = new PbftCommo(poll);
  }

  #ifdef PBFT_TEST_CORO
  pbft_test_mutex_.lock();
  verify(n_replicas_ == 4);
  for (int i = 0; i < 4; i++) {
    if (replicas_[i] == this) {
      verify(n_commo_ < 4);
      n_commo_++;
      break;
    }
  }
  pbft_test_mutex_.unlock();

  if (site_info_->locale_id == 0) {
    verify(pbft_test_coro_.get() == nullptr);
    Log_debug("Creating Pbft test coroutine");
    pbft_test_coro_ = Coroutine::CreateRun([this] () {
      // Yield until all 4 communicators are initialized
      Coroutine::CurrentCoroutine()->Yield();
      // Run tests
      verify(n_replicas_ == 4);
      auto testconfig = new PbftTestConfig(replicas_);
      PbftLabTest test(testconfig);
      test.Run();
      test.Cleanup();
      // Turn off Reactor loop
      Reactor::GetReactor()->looping_ = false;
      return;
    });
    Log_info("pbft_test_coro_ id=%d", pbft_test_coro_->id);
    // wait until n_commo_ == 4, then resume the coroutine
    pbft_test_mutex_.lock();
    while (n_commo_ < 4) {
      pbft_test_mutex_.unlock();
      sleep(0.1);
      pbft_test_mutex_.lock();
    }
    pbft_test_mutex_.unlock();
    Reactor::GetReactor()->ContinueCoro(pbft_test_coro_);
  }
  #endif

  return commo_;
}

vector<rrr::Service *>
PbftFrame::CreateRpcServices(uint32_t site_id,
                                   TxLogServer *rep_sched,
                                   rrr::PollMgr *poll_mgr,
                                   ServerControlServiceImpl *scsi) {
  auto config = Config::GetConfig();
  auto result = std::vector<Service *>();
  switch (config->replica_proto_) {
    case MODE_FPGA_RAFT:result.push_back(new PbftServiceImpl(rep_sched));
    default:break;
  }
  return result;
}

} // namespace janus;
