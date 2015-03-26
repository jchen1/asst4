#include <glog/logging.h>
#include <stdio.h>
#include <stdlib.h>

#include <tuple>
#include <unordered_map>
#include <queue>

#include "server/messages.h"
#include "server/master.h"
#include "tools/cycle_timer.h"

static struct Master_state {

  // The mstate struct collects all the master node state into one
  // place.  You do not need to preserve any of the fields below, they
  // exist only to implement the basic functionality of the starter
  // code.

  bool server_ready;
  int max_workers;
  int next_tag;

  float incoming_rate;
  float outgoing_rate;

  // tag -> (client handle, request timestamp, request type)
  std::unordered_map<int, std::tuple<Client_handle, float, Request_msg> > clients;

  // A simple cache for countprimes (and compareprimes) requests
  std::unordered_map<int, int> primes;

  std::queue<Worker_handle> cpu_queue;
  std::queue<Worker_handle> mem_queue;
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

  for (int i = 0; i < 4; i++) {
    counts[i] = mstate.primes[params[i]];
  }

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
  mstate.max_workers = max_workers;
  mstate.incoming_rate = 0.0f;
  mstate.outgoing_rate = 0.0f;

  // don't mark the server as ready until the server is ready to go.
  // This is actually when the first worker is up and running, not
  // when 'master_node_init' returnes
  mstate.server_ready = false;

  // fire off a request for a new worker
  for (int i = 0; i < max_workers; i++) {
    int tag = random();
    Request_msg req(tag);
    req.set_arg("name", "my worker");
    request_new_worker_node(req);
  }

}


Worker_handle get_best_worker(bool memory_intensive) {
  Worker_handle worker;
  if (memory_intensive) {
    worker = mstate.mem_queue.front();
    mstate.mem_queue.pop();
    mstate.mem_queue.push(worker);
  }
  else {
    worker = mstate.cpu_queue.front();
    mstate.cpu_queue.pop();
    mstate.cpu_queue.push(worker);
  }
  return worker;
}

void handle_new_worker_online(Worker_handle worker_handle, int tag) {

  // 'tag' allows you to identify which worker request this response
  // corresponds to.  Since the starter code only sends off one new
  // worker request, we don't use it here.

  mstate.cpu_queue.push(worker_handle);
  mstate.mem_queue.push(worker_handle);

  // Now that a worker is booted, let the system know the server is
  // ready to begin handling client requests.  The test harness will
  // now start its timers and start hitting your server with requests.
  if (mstate.server_ready == false && mstate.cpu_queue.size() == (unsigned int) mstate.max_workers) {
    mstate.server_ready = true;
    server_init_complete();
  }
}

void handle_worker_response(Worker_handle worker_handle, const Response_msg& resp) {

  int tag = resp.get_tag();

  // Master node has received a response from one of its workers.
  // Here we directly return this response to the client.

  DLOG(INFO) << "Master received a response from a worker: [" << tag << ":" << resp.get_response() << "]" << std::endl;

  Client_handle waiting_client;
  float request_time;
  Request_msg request;
  std::tie(waiting_client, request_time, request) = mstate.clients.at(tag);

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
  else {
    send_client_response(waiting_client, resp);
    mstate.clients.erase(tag);
  }

  // TODO: update response time with exponential moving window
}

void handle_client_request(Client_handle client_handle, const Request_msg& client_req) {

  DLOG(INFO) << "Received request: " << client_req.get_request_string() << std::endl;

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
  mstate.clients.insert(std::make_pair(tag,
    std::tuple<Client_handle, float, Request_msg>(client_handle, time, Request_msg(tag, client_req))));

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

        send_request_to_worker(get_best_worker(false), dummy_req);
        mstate.clients.insert(std::make_pair(tag,
          std::tuple<Client_handle, float, Request_msg>(client_handle, time, dummy_req)));
      }
    }

    // All of the requests were already in the cache! just respond
    if (uncached == 0) {
      calc_and_send_compareprimes(client_handle, client_req);
    }
  } else if (client_req.get_arg("cmd") == "countprimes") {
    int param = atoi(client_req.get_arg("n").c_str());
    if (mstate.primes.count(param) == 0) {
      Request_msg worker_req(tag, client_req);
      send_request_to_worker(get_best_worker(false), worker_req);
    }
    else {
      Response_msg resp(client_req.get_tag());
      char tmp_buffer[32];
      sprintf(tmp_buffer, "%d", mstate.primes.at(param));
      resp.set_response(tmp_buffer);
      send_client_response(client_handle, resp);

    }
  } else if (client_req.get_arg("cmd") == "projectidea") {
    Request_msg worker_req(tag, client_req);
    send_request_to_worker(get_best_worker(true), worker_req);
  } else {
    Request_msg worker_req(tag, client_req);
    send_request_to_worker(get_best_worker(false), worker_req);
  }

  // We're done!  This event handler now returns, and the master
  // process calls another one of your handlers when action is
  // required.

}


void handle_tick() {

  // TODO: you may wish to take action here.  This method is called at
  // fixed time intervals, according to how you set 'tick_period' in
  // 'master_node_init'.

}

