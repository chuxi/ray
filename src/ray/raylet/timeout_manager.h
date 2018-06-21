#ifndef RAY_RAYLET_TIMEOUT_MANAGER_H
#define RAY_RAYLET_TIMEOUT_MANAGER_H

#include "ray/raylet/task.h"
#include "ray/id.h"

#include <unordered_map>

namespace ray {

namespace raylet {

/// \calss TimeoutEntry
///
/// A class that represents an entry of task timeout.
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

/// \class TimeoutManager
///
/// A class to manage task's timeout information.
class TimeoutManager {
public:
  TimeoutManager() = default;
  virtual ~TimeoutManager() = default;

  /// Add a timeout entry into timeout_manager.
  ///
  /// \param task_id The task ID that the task will be added.
  /// \param timeout_budget_millis The task's timeout budget time, and the unit is millis.
  /// \param last_updated_millis The task's last updated time, and the unit is millis.
  void AddTimeoutEntry(const TaskID &task_id,
                       int64_t timeout_budget_millis,
                       int64_t last_updated_millis);

  /// Remove a timeout entry from timeout_manager.
  ///
  /// \param task_id The task ID that the task will be removed.
  void RemoveTimeoutEntry(const TaskID &task_id);

  /// Update the task's timeout budget time.
  ///
  /// \param task_id The task ID that the task's timeout budget time will be updated.
  /// \param timeout_millis The task's timeout time, and the unit is millis.
  /// \param now_millis The time that the task will be updated.
  void UpdateTimeoutBudget(const TaskID &task_id,
                           int64_t timeout_millis,
                           int64_t now_millis);

  /// Update the task's timeout budget time.
  ///
  /// \param task The task's timeout budget time will be updated.
  /// \param now_millis The time that the task will be updated.
  void UpdateTimeoutBudget(const Task &task,
                           int64_t now_millis);

  /// Query if the task is timeout.
  ///
  /// \param task_id The taskID that the task will be queried.
  /// \return If the task is timeout.
  bool Timeout(const TaskID &task_id) const;

  /// Query if the task timeout entry exists.
  ///
  /// \param task_id The task ID that we will query.
  /// \return If the task timeout exists.
  bool TimeoutEntryExists(const TaskID &task_id) const;

  /// Get the task's timeout budget time, and the unit is millis.
  ///
  /// \param task_id The task ID that we will query.
  /// \return The timeout budget millis of the task.
  int64_t TimeoutBudgetMillis(const TaskID &task_id) const;

  /// Get the task's last updated time, and the unit is millis.
  ///
  /// \param task_id The task ID that we will query.
  /// \return The last updated millis of the task.
  int64_t LastUpdatedMillis(const TaskID &task_id) const;

private:
  std::unordered_map<TaskID, TimeoutEntry> task_timeout_info_;
};

} //namespace raylet

} //namespace ray

#endif //RAY_RAYLET_TIMEOUT_MANAGER_H
