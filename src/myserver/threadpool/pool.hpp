// NOTE: I (jchen4) wrote this. See github.com/iambald/threadpool

#ifndef THREADPOOL_POOL_H
#define THREADPOOL_POOL_H

#include <condition_variable>
#include <future>
#include <list>
#include <queue>

#include "worker_thread.hpp"

namespace threadpool {

/*
 * Thread pool that does not need a master thread to manage load. A task is of
 * the form std::function<T(void)>, and the add_task() function will return a
 * std::future<T> which will contain the return value of the function. Tasks are
 * placed in a queue. Threads are created only when there are no idle threads
 * available and the total thread count does not exceed the maximum thread
 * count. Threads are despawned if they are idle for more than despawn_file_ms,
 * the third argument in the constructor of the threadpool.
 */
class pool
{
 public:
  /*
   * Creates a new thread pool, with max_threads threads allowed. Starts paused
   * if start_paused = true. Default values are max_threads =
   * std::thread::hardware_concurrency(), which should return the number of
   * physical cores the CPU has, start_paused = false, and idle_time = 1000.
   */
  pool(unsigned int max_threads = 32,//std::thread::hardware_concurrency(),
       unsigned int idle_time = 0)
    : max_threads(max_threads),
      idle_time(idle_time),
      threads_created(0),
      threads_running(0),
      join_requested(false)
  {
    for (unsigned int i = 0; i < max_threads; i++)
    {
      threads.emplace_back(std::bind(&pool::run_task, this));
    }
  }

  /*
   * When the pool is destructed, it will first stop all worker threads.
   */
  ~pool()
  {
    join();
  }

  /*
   * Adds a new task to the task queue. The task must be a function object,
   * and the remaining passed arguments must be parameters for task, which will
   * be bound using std::bind().
   */
  template <typename T, typename... Args,
            typename R = typename std::result_of<T(Args...)>::type>
  std::future<R> add_task(T&& task, Args&&... args)
  {
    /*
     * If all created threads are executing tasks and we have not spawned the
     * maximum number of allowed threads, create a new thread.
     */
    // if (threads_created == threads_running && threads_created != max_threads)
    // {
    //   std::lock_guard<std::mutex> thread_lock(thread_mutex);
    //   threads.emplace_back(std::bind(&pool::run_task, this));
    // }
    auto p_task = std::make_shared<std::packaged_task<R()>>(
      std::bind(std::forward<T>(task), std::forward<Args>(args)...));
    {
      std::lock_guard<std::mutex> task_lock(task_mutex);
      tasks.emplace([p_task] { (*p_task)(); });
    }
    task_ready.notify_one();

    return p_task->get_future();
  }

  /*
   * Clears the task queue. Does not stop any running tasks.
   */
  void clear()
  {
    std::lock_guard<std::mutex> task_lock(task_mutex);
    std::queue<std::function<void(void)>>().swap(tasks);
  }

  /*
   * Returns true if the task queue is empty. Note that worker threads may be
   * running, even if empty() returns true.
   */
  bool empty()
  {
    std::lock_guard<std::mutex> task_lock(task_mutex);
    return tasks.empty();
  }

  /*
   * Waits for all threads to finish executing. All worker threads will exit.
   */
  void join()
  {
    join_requested = true;

    task_ready.notify_all();
    {
      std::lock_guard<std::mutex> thread_lock(thread_mutex);
      for (auto&& thread : threads)
      {
        thread.join();
      }
      threads.clear();
    }
    join_requested = false;
  }

  /*
   * Waits for the task queue to empty and for all worker threads to complete,
   * without destroying worker threads.
   */
  void wait(bool clear_tasks = false)
  {
    if (clear_tasks)
    {
      clear();
    }
    std::unique_lock<std::mutex> lck(task_mutex);
    task_empty.wait(lck, [&] { return tasks.empty() && !threads_running; });
  }

  unsigned int get_threads_running() const
  {
    return threads_running.load();
  }

  unsigned int get_threads_created() const
  {
    return threads_created.load();
  }

  unsigned int get_max_threads() const
  {
    return max_threads;
  }

  void set_max_threads(unsigned int max_threads)
  {
    this->max_threads = max_threads;
  }

  unsigned int get_idle_time() const
  {
    return idle_time.count();
  }

  void set_idle_time(unsigned int idle_time)
  {
    this->idle_time = std::chrono::milliseconds(idle_time);
  }

 private:
  std::function<void(void)> pop_task()
  {
    std::function<void(void)> ret;
    std::unique_lock<std::mutex> task_lock(task_mutex);
    if (idle_time.count() > 0)
    {
      task_ready.wait_for(task_lock, idle_time,
        [&]{ return join_requested || !tasks.empty(); });
    }
    else
    {
      task_ready.wait(task_lock,
        [&]{ return join_requested || !tasks.empty(); });
    }
    if (!tasks.empty())
    {
      ret = tasks.front();
      tasks.pop();
    }

    return ret;
  }

  void run_task()
  {
    ++threads_created;
    while (threads_created <= max_threads)
    {
      if (auto t = pop_task())
      {
        ++threads_running;
        t();
        --threads_running;

        // if (!threads_running && empty())
        // {
        //   task_empty.notify_all();
        // }
      }
      else
      {
        break;
      }
    }
    --threads_created;

    // if (std::unique_lock<std::mutex>(thread_mutex, std::try_to_lock))
    // {
    //   threads.remove_if([] (const worker_thread& thread) {
    //     return !thread;
    //   });
    // }
  }

  std::list<worker_thread> threads;
  std::queue<std::function<void(void)>> tasks;

  std::mutex task_mutex, thread_mutex;
  std::condition_variable_any task_ready, task_empty;

  unsigned int max_threads;
  std::chrono::milliseconds idle_time;

  std::atomic<unsigned int> threads_created, threads_running;
  std::atomic<bool> join_requested;
};

}

#endif //THREADPOOL_POOL_H
