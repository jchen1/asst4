
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <sstream>
#include <glog/logging.h>

#include <future>

#include "server/messages.h"
#include "server/worker.h"
#include "tools/cycle_timer.h"
#include "threadpool/pool.hpp"

static struct Worker_state {

  // The mstate struct collects all the master node state into one
  // place.  You do not need to preserve any of the fields below, they
  // exist only to implement the basic functionality of the starter
  // code.

  threadpool::pool tp;
} wstate;

// Generate a valid 'countprimes' request dictionary from integer 'n'
static void create_computeprimes_req(Request_msg& req, int n) {
  std::ostringstream oss;
  oss << n;
  req.set_arg("cmd", "countprimes");
  req.set_arg("n", oss.str());
}

void worker_node_init(const Request_msg& params) {

  // This is your chance to initialize your worker.  For example, you
  // might initialize a few data structures, or maybe even spawn a few
  // pthreads here.  Remember, when running on Amazon servers, worker
  // processes will run on an instance with a dual-core CPU.

  DLOG(INFO) << "**** Initializing worker: " << params.get_arg("name") << " ****\n";

}

void worker_handle_request(const Request_msg& req) {

  // Make the tag of the reponse match the tag of the request.  This
  // is a way for your master to match worker responses to requests.

  // Output debugging help to the logs (in a single worker node
  // configuration, this would be in the log logs/worker.INFO)
  DLOG(INFO) << "Worker got request: [" << req.get_tag() << ":" << req.get_request_string() << "]\n";

  // Add the task to the threadpool.
  wstate.tp.add_task([=]{
    Response_msg resp(req.get_tag());
    double startTime = CycleTimer::currentSeconds();
    execute_work(req, resp);
    double dt = CycleTimer::currentSeconds() - startTime;
    DLOG(INFO) << "Worker completed work in " << (1000.f * dt) << " ms (" << req.get_tag()  << ", " << resp.get_response() << ")\n";

    // send a response string to the master
    worker_send_response(resp);
  });

}
