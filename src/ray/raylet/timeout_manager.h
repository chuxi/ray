#ifndef RAY_RAYLET_TIMEOUT_MANAGER_H
#define RAY_RAYLET_TIMEOUT_MANAGER_H

#include "ray/raylet/task.h"
#include "ray/id.h"

#include <unordered_map>

namespace ray {

namespace raylet {

struct TimeoutEntry {
  int64_t timeout_budget_millis;
  int64_t last_updated_millis;

  TimeoutEntry() = default;
  TimeoutEntry(int64_t timeout_budget_millis,
               int64_t last_updated_millis) {
    this->timeout_budget_millis = timeout_budget_millis;
    this->last_updated_millis = last_updated_millis;
  }
};

class TimeoutManager {
public:
  TimeoutManager() = default;
  virtual ~TimeoutManager() = default;

  void AddTimeoutEntry(const TaskID &task_id,
                       int64_t timeout_budget_millis,
                       int64_t last_updated_millis);

  void RemoveTimeoutEntry(const TaskID &task_id);

  void UpdateTimeoutBudget(const TaskID &task_id,
                           int64_t timeout_millis,
                           int64_t now_millis);

  void UpdateTimeoutBudget(const Task &task,
                           int64_t now_millis);

  bool Timeout(const TaskID &task_id) const;

  bool TimeoutEntryExists(const TaskID &task_id) const;

  int64_t TimeoutBudgetMillis(const TaskID &task_id) const;

  int64_t LastUpdatedMillis(const TaskID &task_id) const;

private:
  std::unordered_map<TaskID, TimeoutEntry> task_timeout_info_;
};

} //namespace raylet

} //namespace ray

#endif //RAY_RAYLET_TIMEOUT_MANAGER_H
