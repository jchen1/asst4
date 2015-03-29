#include <glog/logging.h>
#include <stdio.h>
#include <stdlib.h>

#include <algorithm>
#include <functional>
#include <tuple>
#include <unordered_map>
#include <queue>

#include "server/messages.h"
#include "server/master.h"
#include "tools/cycle_timer.h"

using worker_t = std::tuple<Worker_handle, int, int, bool>;

#define MAX_MEM_REQUESTS 2
#define MAX_CPU_REQUESTS 32

static bool compare_request_priorities(Request_msg a, Request_msg b) {
  if (a.get_arg("cmd") == "tellmenow" && b.get_arg("cmd") != "tellmenow") {
    return true;
  }
  else if (a.get_arg("cmd") != "tellmenow" && b.get_arg("cmd") == "tellmenow") {
    return false;
  }

  return (a.get_tag() > b.get_tag());
}


static struct Master_state {

  // The mstate struct collects all the master node state into one
  // place.  You do not need to preserve any of the fields below, they
  // exist only to implement the basic functionality of the starter
  // code.

  bool server_ready;
  int min_workers, max_workers;
  int next_tag;

  float incoming_rate;
  float outgoing_rate;

  // tag -> (client handle, request timestamp, request)
  std::unordered_map<int, std::tuple<Client_handle, float, Request_msg> > clients;

  // A simple cache for countprimes (and compareprimes) requests
  std::unordered_map<int, int> primes;

  // Request priority queue - prioritize tellmenow requests, then sort by tag
  std::priority_queue<Request_msg,
                      std::vector<Request_msg>,
                      decltype(&compare_request_priorities)> request_cpu_queue;

  // Priority queue for memory requests: always sort by tag
  std::priority_queue<Request_msg,
                      std::vector<Request_msg>,
                      decltype(&compare_request_priorities)> request_mem_queue;

  // Vector of tuple (worker, pending cpu requests, pending mem requests, worker dying)
  std::vector<worker_t> workers;

} mstate;


// Generate a valid 'countprimes' request dictionary from integer 'n'
static void create_computeprimes_req(Request_msg& req, int n) {
  std::ostringstream oss;
  oss << n;
  req.set_arg("cmd", "countprimes");
  req.set_arg("n", oss.str());
}

// NOTE: this function should not be called until all of the countprime requests have finished
// Otherwise it will probably give incorrect results
void calc_and_send_compareprimes(Client_handle client_handle, const Request_msg& client_req) {
  Response_msg response(client_req.get_tag());
  int counts[4], params[4];

  // grab the four arguments defining the two ranges
  params[0] = atoi(client_req.get_arg("n1").c_str());
  params[1] = atoi(client_req.get_arg("n2").c_str());
  params[2] = atoi(client_req.get_arg("n3").c_str());
  params[3] = atoi(client_req.get_arg("n4").c_str());

  DLOG(INFO) << "Fulfilling compareprimes request " << client_req.get_tag() << ": (" << params[0] << ", " << params[1] << ", " << params[2] << ", " << params[3] << ")\n";

  std::transform(params, params + 4, counts, [&](const int param) {
    return mstate.primes.at(param);
  });

  if (counts[1]-counts[0] > counts[3]-counts[2])
    response.set_response("There are more primes in first range.");
  else
    response.set_response("There are more primes in second range.");
  send_client_response(client_handle, response);

  mstate.clients.erase(client_req.get_tag());
}


void master_node_init(int max_workers, int& tick_period) {

  // set up tick handler to fire every 5 seconds. (feel free to
  // configure as you please)
  tick_period = 2;

  mstate.next_tag = 0;
  mstate.min_workers = 1;
  mstate.max_workers = max_workers;
  mstate.incoming_rate = 0.0f;
  mstate.outgoing_rate = 0.0f;

  // don't mark the server as ready until the server is ready to go.
  // This is actually when the first worker is up and running, not
  // when 'master_node_init' returnes
  mstate.server_ready = false;

  mstate.request_cpu_queue =  std::priority_queue<Request_msg,
                              std::vector<Request_msg>,
                              decltype(&compare_request_priorities)>(&compare_request_priorities);

  mstate.request_mem_queue =  std::priority_queue<Request_msg,
                              std::vector<Request_msg>,
                              decltype(&compare_request_priorities)>(&compare_request_priorities);

  // fire off a request for a new worker
  int tag = random();
  Request_msg req(tag);
  req.set_arg("name", "my worker");
  request_new_worker_node(req);

}

// Returns a reference to the best worker tuple, so we can modify its task counts
worker_t& get_best_worker(bool memory_intensive) {
  auto comparator = [&](worker_t a, worker_t b) {

    // If either of the workers is dying, return the live one
    if (std::get<3>(a) && !std::get<3>(b)) return false;
    else if (std::get<3>(b) && !std::get<3>(a)) return true;

    if (memory_intensive) {
      return std::get<2>(a) < std::get<2>(b);
    }
    else {
      return std::get<1>(a) < std::get<1>(b);
    }
  };

  return *std::min_element(std::begin(mstate.workers), std::end(mstate.workers), comparator);
}

void route_request(const Request_msg& request) {
  // if free workers, assign them
  // else, add it to the queue
  auto& worker_tuple = get_best_worker(request.get_arg("cmd") == "projectidea");
  auto worker_handle = std::get<0>(worker_tuple);

  if (request.get_arg("cmd") == "projectidea") {
    int current_requests = std::get<2>(worker_tuple);
    if (current_requests >= MAX_MEM_REQUESTS || std::get<3>(worker_tuple)) {
      mstate.request_mem_queue.push(request);
    }
    else {
      send_request_to_worker(worker_handle, request);
      std::get<2>(worker_tuple)++;
    }
  }
  else {
    int current_requests = std::get<1>(worker_tuple) + std::get<2>(worker_tuple);
    if (current_requests >= MAX_CPU_REQUESTS || std::get<3>(worker_tuple)) {
      mstate.request_cpu_queue.push(request);
    }
    else {
      send_request_to_worker(worker_handle, request);
      std::get<1>(worker_tuple)++;
    }
  }

}

void handle_new_worker_online(Worker_handle worker_handle, int tag) {

  // 'tag' allows you to identify which worker request this response
  // corresponds to.  Since the starter code only sends off one new
  // worker request, we don't use it here.

  mstate.workers.push_back(std::make_tuple(worker_handle, 0, 0, false));

  // Now that a worker is booted, let the system know the server is
  // ready to begin handling client requests.  The test harness will
  // now start its timers and start hitting your server with requests.
  if (mstate.server_ready == false) {
    mstate.server_ready = true;
    server_init_complete();
  }
}

void handle_worker_response(Worker_handle worker_handle, const Response_msg& resp) {

  int tag = resp.get_tag();

  DLOG(INFO) << "Master received a response from a worker: [" << tag << ":" << resp.get_response() << "]" << std::endl;

  Client_handle waiting_client;
  float request_time;
  Request_msg request;
  std::tie(waiting_client, request_time, request) = mstate.clients.at(tag);

  auto& worker_tuple = *(std::find_if(std::begin(mstate.workers), std::end(mstate.workers), [&](decltype(*std::begin(mstate.workers)) tuple) {
    return std::get<0>(tuple) == worker_handle;
  }));

  // If there is
  if (request.get_arg("cmd") == "projectidea") {
    std::get<2>(worker_tuple)--;
    if (!mstate.request_mem_queue.empty()) {
      auto new_request = mstate.request_mem_queue.top();
      mstate.request_mem_queue.pop();

      route_request(new_request);
    }
  }
  else {
    std::get<1>(worker_tuple)--;
    if (!mstate.request_cpu_queue.empty()) {
      auto new_request = mstate.request_cpu_queue.top();
      mstate.request_cpu_queue.pop();

      route_request(new_request);
    }
  }

  if (request.get_arg("cmd") == "countprimes") {
    int ans = atoi(resp.get_response().c_str());
    mstate.primes[atoi(request.get_arg("n").c_str())] = ans;
    mstate.clients.erase(tag);

    // Check if we are getting a compareprimes result
    int parent_tag = tag - (tag % 5);
    if (mstate.clients.count(parent_tag) > 0) {
      Request_msg parent_req = std::get<2>(mstate.clients.at(parent_tag));
      if (parent_req.get_arg("cmd") == "compareprimes") {
        // Check if the compareprimes result is ready
        bool done = true;
        for (int i = 1; i < 5; i++) {
          if (mstate.clients.count(parent_tag + i) > 0) {
            done = false;
          }
        }

        if (done) {
          calc_and_send_compareprimes(waiting_client, parent_req);
        }

        return;
      }

    }

    // If we get here, it is a countprimes request directly from the server
    send_client_response(waiting_client, resp);

  }
  else if (request.get_arg("cmd") == "projectidea") {
    send_client_response(waiting_client, resp);
    mstate.clients.erase(tag);
  }
  else {
    send_client_response(waiting_client, resp);
    mstate.clients.erase(tag);
  }

  // TODO: update response time with exponential moving window
}

void handle_client_request(Client_handle client_handle, const Request_msg& client_req) {

  // You can assume that traces end with this special message.  It
  // exists because it might be useful for debugging to dump
  // information about the entire run here: statistics, etc.
  if (client_req.get_arg("cmd") == "lastrequest") {
    Response_msg resp(0);
    resp.set_response("ack");
    send_client_response(client_handle, resp);
    return;
  }

  // Save off the handle to the client that is expecting a response.
  // The master needs to do this it can response to this client later
  // when 'handle_worker_response' is called.
  int tag = mstate.next_tag++;
  float time = CycleTimer::currentSeconds();

  if (client_req.get_arg("cmd") == "compareprimes") {
    tag = tag + 4 - ((tag - 1) % 5);
    mstate.next_tag = tag + 5;
  }

  DLOG(INFO) << "Received request[" << tag << "]: " << client_req.get_request_string() << std::endl;


  Request_msg worker_req(tag, client_req);

  mstate.clients.insert(std::make_pair(tag,
    std::make_tuple(client_handle, time, worker_req)));

  // Fire off the request to the worker.  Eventually the worker will
  // respond, and your 'handle_worker_response' event handler will be
  // called to forward the worker's response back to the server.
  if (client_req.get_arg("cmd") == "compareprimes") {
    int params[4];
    int uncached = 0;

    // grab the four arguments defining the two ranges
    params[0] = atoi(client_req.get_arg("n1").c_str());
    params[1] = atoi(client_req.get_arg("n2").c_str());
    params[2] = atoi(client_req.get_arg("n3").c_str());
    params[3] = atoi(client_req.get_arg("n4").c_str());

    for (int i=0; i<4; i++) {
      tag++;
      if (mstate.primes.count(params[i]) == 0) {
        uncached++;
        Request_msg dummy_req(tag);
        create_computeprimes_req(dummy_req, params[i]);
        route_request(dummy_req);


        // send_request_to_worker(get_best_worker(false), dummy_req);
        mstate.clients.insert(std::make_pair(tag,
          std::make_tuple(client_handle, time, dummy_req)));
      }
    }

    // All of the requests were already in the cache! just respond
    if (uncached == 0) {
      calc_and_send_compareprimes(client_handle, client_req);
    }
  } else if (client_req.get_arg("cmd") == "countprimes") {
    int param = atoi(client_req.get_arg("n").c_str());
    if (mstate.primes.count(param) == 0) {
      route_request(worker_req);
    }
    else {
      Response_msg resp(client_req.get_tag());
      char tmp_buffer[32];
      sprintf(tmp_buffer, "%d", mstate.primes.at(param));
      resp.set_response(tmp_buffer);
      send_client_response(client_handle, resp);
    }
  } else if (client_req.get_arg("cmd") == "projectidea") {
    route_request(worker_req);
  } else {
    route_request(worker_req);
  }

  // We're done!  This event handler now returns, and the master
  // process calls another one of your handlers when action is
  // required.

}

#define MIN_CPU_MULT 2
#define MIN_MEM_MULT 2

#define MAX_CPU_MULT 0.5f
#define MAX_MEM_MULT 0.5f


void handle_tick() {

  // DLOG(INFO) << "Current queue sizes: CPU " << mstate.request_cpu_queue.size() << " MEM " << mstate.request_mem_queue.size() << std::endl;

  // TODO: you may wish to take action here.  This method is called at
  // fixed time intervals, according to how you set 'tick_period' in
  // 'master_node_init'.

  // Kill any workers that are marked for deletion and have no more jobs to complete
  std::remove_if(std::begin(mstate.workers), std::end(mstate.workers), [](worker_t worker) {
    if (std::get<3>(worker) && (std::get<1>(worker) + std::get<2>(worker)) == 0) {
      kill_worker_node(std::get<0>(worker));
      return true;
    }
    return false;
  });

  int live_workers = std::count_if(std::begin(mstate.workers), std::end(mstate.workers), [](const worker_t& worker) {
    return !std::get<3>(worker);
  });

  int cpu_throughput = live_workers * MAX_CPU_REQUESTS;
  int mem_throughput = live_workers * MAX_MEM_REQUESTS;

  // If both job queues are small compared to live workers count and workers.size() > mstate.min_workers,
  // mark the live worker with the fewest jobs for deletion

  if (live_workers > mstate.min_workers &&
      cpu_throughput > mstate.request_cpu_queue.size() * MIN_CPU_MULT &&
      mem_throughput > mstate.request_mem_queue.size() * MIN_MEM_MULT) {

    auto comparator = [&](worker_t a, worker_t b) {

      // If either of the workers is dying, return the live one
      if (std::get<3>(a) && !std::get<3>(b)) return false;
      else if (std::get<3>(b) && !std::get<3>(a)) return true;

      return (std::get<1>(a) + std::get<2>(a)) < (std::get<1>(b) + std::get<2>(b));
    };

    auto& worker_to_delete = *std::min_element(std::begin(mstate.workers), std::end(mstate.workers), comparator);
    std::get<3>(worker_to_delete) = true;

    DLOG(INFO) << "Deleting worker " << std::get<0>(worker_to_delete) << std::endl;
  }

  // If any job queue is large and workers.size() < mstate.max_workers, add (up to) 2 workers to handle a burst
  else if ( live_workers < mstate.max_workers &&
            cpu_throughput < mstate.request_cpu_queue.size() * MAX_CPU_MULT &&
            mem_throughput < mstate.request_mem_queue.size() * MAX_MEM_MULT) {
    int workers_to_add = std::min(2, (int) (mstate.max_workers - mstate.workers.size()));
    for (int i = 0; i < workers_to_add; i++) {
      int tag = random();
      Request_msg req(tag);
      req.set_arg("name", "my worker");
      request_new_worker_node(req);
    }

    DLOG(INFO) << "Adding " << workers_to_add << "workers " << std::endl;

  }


}

