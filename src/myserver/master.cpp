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

  // tag -> (client handle, request timestamp)
  std::unordered_map<int, std::tuple<Client_handle, float> > clients;
  std::queue<Worker_handle> cpu_queue;
  std::queue<Worker_handle> mem_queue;
} mstate;



void master_node_init(int max_workers, int& tick_period) {

  // set up tick handler to fire every 5 seconds. (feel free to
  // configure as you please)
  tick_period = 5;

  mstate.next_tag = 0;
  mstate.max_workers = max_workers;
  mstate.clients = std::unordered_map<int, std::tuple<Client_handle, float>>();
  mstate.cpu_queue = std::queue<Worker_handle>();
  mstate.mem_queue = std::queue<Worker_handle>();
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

void handle_new_worker_online(Worker_handle worker_handle, int tag) {

  // 'tag' allows you to identify which worker request this response
  // corresponds to.  Since the starter code only sends off one new
  // worker request, we don't use it here.

  mstate.cpu_queue.push(worker_handle);
  mstate.mem_queue.push(worker_handle);

  // Now that a worker is booted, let the system know the server is
  // ready to begin handling client requests.  The test harness will
  // now start its timers and start hitting your server with requests.
  if (mstate.server_ready == false && mstate.cpu_queue.size() == mstate.max_workers) {
    mstate.server_ready = true;
    server_init_complete();
  }
}

void handle_worker_response(Worker_handle worker_handle, const Response_msg& resp) {

  // Master node has received a response from one of its workers.
  // Here we directly return this response to the client.

  DLOG(INFO) << "Master received a response from a worker: [" << resp.get_tag() << ":" << resp.get_response() << "]" << std::endl;

  Client_handle waiting_client;
  float request_time;
  std::tie(waiting_client, request_time) = mstate.clients.at(resp.get_tag());

  send_client_response(waiting_client, resp);
  // TODO: update response time with exponential moving window
  mstate.clients.erase(resp.get_tag());
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
  mstate.clients.insert(std::make_pair(tag, std::tuple<Client_handle, float>(client_handle, time)));

  // Fire off the request to the worker.  Eventually the worker will
  // respond, and your 'handle_worker_response' event handler will be
  // called to forward the worker's response back to the server.
  Worker_handle my_worker;
  if (client_req.get_arg("cmd") == "projectidea") {
    my_worker = mstate.mem_queue.front();
    mstate.mem_queue.pop();
    mstate.mem_queue.push(my_worker);
  } else {
    my_worker = mstate.cpu_queue.front();
    mstate.cpu_queue.pop();
    mstate.cpu_queue.push(my_worker);
  }

  Request_msg worker_req(tag, client_req);
  send_request_to_worker(my_worker, worker_req);

  // We're done!  This event handler now returns, and the master
  // process calls another one of your handlers when action is
  // required.

}


void handle_tick() {

  // TODO: you may wish to take action here.  This method is called at
  // fixed time intervals, according to how you set 'tick_period' in
  // 'master_node_init'.

}

