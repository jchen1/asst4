#ifndef THREADPOOL_WORKERTHREAD_H
#define THREADPOOL_WORKERTHREAD_H

#include <thread>

namespace threadpool {

class worker_thread
{
 public:
  worker_thread(std::function<void(void)> run_task)
    : thread(std::bind(&worker_thread::run, this, run_task)), running(true) {}

  ~worker_thread()
  {
    join();
  }

  void join()
  {
    if (thread.joinable())
    {
      thread.join();
    }
  }

  operator bool() const
  {
    return running;
  }

 private:
  void run(std::function<void(void)> run_task)
  {
    run_task();
    running = false;
  }

  std::thread thread;
  bool running;
};

}

#endif //THREADPOOL_WORKERTHREAD_H
