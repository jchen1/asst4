
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <sstream>
#include <glog/logging.h>

#include <atomic>

#include "server/messages.h"
#include "server/worker.h"
#include "tools/cycle_timer.h"
#include "threadpool/pool.hpp"
#include <pthread.h>

static struct Worker_state {

  // The mstate struct collects all the master node state into one
  // place.  You do not need to preserve any of the fields below, they
  // exist only to implement the basic functionality of the starter
  // code.

  threadpool::pool tp;

  std::atomic<bool> use_first_cpu;

} wstate;

void worker_node_init(const Request_msg& params) {

  // This is your chance to initialize your worker.  For example, you
  // might initialize a few data structures, or maybe even spawn a few
  // pthreads here.  Remember, when running on Amazon servers, worker
  // processes will run on an instance with a dual-core CPU.

  wstate.use_first_cpu = true;

  DLOG(INFO) << "**** Initializing worker: " << params.get_arg("name") << " ****\n";

}

void worker_handle_request(const Request_msg& req) {

  // Make the tag of the reponse match the tag of the request.  This
  // is a way for your master to match worker responses to requests.

  // Output debugging help to the logs (in a single worker node
  // configuration, this would be in the log logs/worker.INFO)
  // DLOG(INFO) << "Worker got request: [" << req.get_tag() << ":" << req.get_request_string() << "]\n";

  // Add the task to the threadpool.
  wstate.tp.add_task([req]{
    Response_msg resp(req.get_tag());
    double startTime = CycleTimer::currentSeconds();
    if (req.get_arg("cmd") == "projectidea") {
      bool old_use_first_cpu = wstate.use_first_cpu.load(std::memory_order_relaxed);
      while (!wstate.use_first_cpu.compare_exchange_weak(old_use_first_cpu, !old_use_first_cpu));

      cpu_set_t cpuset;
      CPU_ZERO(&cpuset);
      CPU_SET(old_use_first_cpu ? 0 : 1, &cpuset);

      pthread_setaffinity_np(pthread_self(), CPU_SETSIZE, &cpuset);
    }
    execute_work(req, resp);
    double dt = CycleTimer::currentSeconds() - startTime;
    // DLOG(INFO) << "Worker completed work in " << (1000.f * dt) << " ms (" << req.get_tag()  << ", " << resp.get_response() << ")\n";

    // send a response string to the master
    worker_send_response(resp);
  });

}
