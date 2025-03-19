
#include "../marshallable.h"
#include "service.h"
#include "server.h"

namespace janus {

PbftServiceImpl::PbftServiceImpl(TxLogServer *sched)
    : svr_((PbftServer*)sched) {
	struct timespec curr_time;
	clock_gettime(CLOCK_MONOTONIC_RAW, &curr_time);
	srand(curr_time.tv_nsec);
}

void PbftServiceImpl::HandlePreprepare(const PreprepareMessage& mesg,
                                       const MarshallDeputy& md_cmd,
                                       const uint64_t& timestamp,
                                       const cliid_t& client_id,
                                       rrr::DeferredReply* defer) {
  shared_ptr<Marshallable> cmd = const_cast<MarshallDeputy&>(md_cmd).sp_data_;
  svr_->OnPreprepare(mesg, cmd, timestamp, client_id, std::bind(&rrr::DeferredReply::reply, defer)); 
}

void PbftServiceImpl::HandlePrepare(const PrepareMessage& mesg,
                                    rrr::DeferredReply* defer) {
  svr_->OnPrepare(mesg, std::bind(&rrr::DeferredReply::reply, defer));
}

void PbftServiceImpl::HandleCommit(const CommitMessage& mesg,
                                   rrr::DeferredReply* defer) {
  svr_->OnCommit(mesg, std::bind(&rrr::DeferredReply::reply, defer));
}

void PbftServiceImpl::HandleCheckpoint(const CheckpointMessage& mesg,
                                       rrr::DeferredReply* defer) {
  svr_->OnCheckpoint(mesg, std::bind(&rrr::DeferredReply::reply, defer));
}

void PbftServiceImpl::HandleViewChange(const ViewChangeMessage& mesg,
                                       rrr::DeferredReply* defer) {
  svr_->OnViewChange(mesg, std::bind(&rrr::DeferredReply::reply, defer));
}

void PbftServiceImpl::HandleNewView(const NewViewMessage& mesg,
                                    rrr::DeferredReply* defer) {
  svr_->OnNewView(mesg, std::bind(&rrr::DeferredReply::reply, defer));
}

} // namespace janus;
