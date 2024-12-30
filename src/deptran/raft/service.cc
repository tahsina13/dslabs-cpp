
#include "../marshallable.h"
#include "service.h"
#include "server.h"

namespace janus {

RaftServiceImpl::RaftServiceImpl(TxLogServer *sched)
    : svr_((RaftServer*)sched) {
	struct timespec curr_time;
	clock_gettime(CLOCK_MONOTONIC_RAW, &curr_time);
	srand(curr_time.tv_nsec);
}


void RaftServiceImpl::HandleRequestVote(const uint64_t& term,
                                        const locid_t& candidate_id,
                                        const uint64_t& last_log_index,
                                        const uint64_t& last_log_term,
                                        uint64_t *ret_term,
                                        bool_t *vote_granted,
                                        rrr::DeferredReply* defer) {
  /* Your code here */
  svr_->OnRequestVote(term, 
                      candidate_id, 
                      last_log_index, 
                      last_log_term, 
                      ret_term, 
                      vote_granted,
                      std::bind(&rrr::DeferredReply::reply, defer));
}

void RaftServiceImpl::HandleAppendEntries(const uint64_t& term,
                                          const locid_t& leader_id,
                                          const uint64_t& prev_log_index,
                                          const uint64_t& prev_log_term,
                                          const vector<RaftDataWrapper>& entries_wrapper,
                                          const uint64_t& leader_commit,
                                          bool_t *followerAppendOK,
                                          rrr::DeferredReply* defer) {
  /* Your code here */
  std::vector<RaftData> entries;
  std::transform(entries_wrapper.begin(), entries_wrapper.end(), std::back_inserter(entries), 
    [](const RaftDataWrapper& entry) {
      std::shared_ptr<Marshallable> cmd = const_cast<MarshallDeputy&>(entry.cmd).sp_data_;
      return RaftData{cmd, entry.term};
    });
  svr_->OnAppendEntries(term, 
                        leader_id, 
                        prev_log_index, 
                        prev_log_term, 
                        entries,
                        leader_commit, 
                        followerAppendOK,
                        std::bind(&rrr::DeferredReply::reply, defer));
}

void RaftServiceImpl::HandleEmptyAppendEntries(const uint64_t& term,
                                               const locid_t& leader_id,
                                               const uint64_t& leader_commit,
                                               rrr::DeferredReply* defer) {
  /* Your code here */
  svr_->OnEmptyAppendEntries(term, 
                             leader_id,
                             leader_commit, 
                             std::bind(&rrr::DeferredReply::reply, defer));
}

void RaftServiceImpl::HandleHelloRpc(const string& req,
                                     string* res,
                                     rrr::DeferredReply* defer) {
  /* Your code here */
  Log_info("receive an rpc: %s", req.c_str());
  *res = "world";
  defer->reply();
}

} // namespace janus;
