
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

void PbftServiceImpl::HandlePreprepare(const PreprepareRequest& req,
                                       const Message& mesg,
                                       rrr::DeferredReply* defer) {
  svr_->OnPreprepare(req, mesg, std::bind(&rrr::DeferredReply::reply, defer)); 
}

void PbftServiceImpl::HandlePrepare(const PrepareRequest& req,
                                    rrr::DeferredReply* defer) {
  svr_->OnPrepare(req, std::bind(&rrr::DeferredReply::reply, defer));
}

void PbftServiceImpl::HandleCommit(const CommitRequest& req,
                                   rrr::DeferredReply* defer) {
  svr_->OnCommit(req, std::bind(&rrr::DeferredReply::reply, defer));
}

void PbftServiceImpl::HandlePrepared(const PreparedRequest& req,
                                     rrr::DeferredReply* defer) {
  svr_->OnPrepared(req, std::bind(&rrr::DeferredReply::reply, defer));
}

void PbftServiceImpl::HandleCommitted(const CommittedRequest& req,
                                      rrr::DeferredReply* defer) {
  svr_->OnCommitted(req, std::bind(&rrr::DeferredReply::reply, defer));
}

void PbftServiceImpl::HandleCheckpoint(const CheckpointRequest& req,
                                       rrr::DeferredReply* defer) {
  svr_->OnCheckpoint(req, std::bind(&rrr::DeferredReply::reply, defer));
}

void PbftServiceImpl::HandleViewChange(const ViewChangeRequest& req,
                                       rrr::DeferredReply* defer) {
  svr_->OnViewChange(req, std::bind(&rrr::DeferredReply::reply, defer));
}

void PbftServiceImpl::HandleNewView(const NewViewRequest& req,
                                    rrr::DeferredReply* defer) {
  svr_->OnNewView(req, std::bind(&rrr::DeferredReply::reply, defer));
}

} // namespace janus;
