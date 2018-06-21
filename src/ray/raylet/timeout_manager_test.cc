#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "ray/raylet/timeout_manager.h"
#include "ray/raylet/task.h"
#include "ray/raylet/task_execution_spec.h"
#include "ray/raylet/task_spec.h"

namespace ray {

namespace raylet {

class MockNodeManager {
public:
  MockNodeManager() = default;
  ~MockNodeManager() = default;

  void SubmitTask(const Task &task, int64_t timeout_budget_millis, int64_t now_millis) {
    auto task_spec = task.GetTaskSpecification();
    auto task_id = task_spec.TaskId();

    if (!timeout_manager_.TimeoutEntryExists(task_id)) {
      timeout_manager_.AddTimeoutEntry(task_id, timeout_budget_millis, now_millis);
      tasks_timeout_millis_.insert(std::pair<TaskID, int64_t>(task_id, task_spec.TimeoutMillis()));
    } else {
      timeout_manager_.UpdateTimeoutBudget(task_id, timeout_budget_millis, now_millis);
      tasks_timeout_millis_[task_id] = task_spec.TimeoutMillis();
    }
  }

  void ForwardTaskToOtherNode(const Task &task, MockNodeManager &other_node, int64_t now_millis) {
    // Remove from this node.
    auto task_spec = task.GetTaskSpecification();
    auto task_id = task_spec.TaskId();
    auto timeout_budget = timeout_manager_.TimeoutBudgetMillis(task_id);

    auto it = tasks_timeout_millis_.find(task_id);
    tasks_timeout_millis_.erase(it);
    timeout_manager_.RemoveTimeoutEntry(task_id);

    // Assign to another node.
    other_node.SubmitTask(task, timeout_budget, now_millis);
  }

  void ResetTaskUpdateTime(const TaskID &task_id, int64_t now_millis) {
    if (!timeout_manager_.TimeoutEntryExists(task_id)) {
      return ;
    }

    auto timeout_millis = tasks_timeout_millis_[task_id];
    timeout_manager_.UpdateTimeoutBudget(task_id, timeout_millis, now_millis);
  }

  int64_t TimeoutBudgetMillis(const TaskID &task_id) {
    return timeout_manager_.TimeoutBudgetMillis(task_id);
  }

  bool Timeout(const TaskID &task_id) {
    return timeout_manager_.Timeout(task_id);
  }

private:
  TimeoutManager timeout_manager_;
  std::unordered_map<TaskID, int64_t> tasks_timeout_millis_;
};

static inline Task ExampleTask(const std::vector<ObjectID> &arguments,
                               int64_t num_returns,
                               int64_t timeout_millis) {
  std::unordered_map<std::string, double> required_resources;
  std::vector<std::shared_ptr<TaskArgument>> task_arguments;
  for (auto &argument : arguments) {
    std::vector<ObjectID> references = {argument};
    task_arguments.emplace_back(std::make_shared<TaskArgumentByReference>(references));
  }
  auto spec = TaskSpecification(UniqueID::nil(), UniqueID::from_random(), 0,
                                UniqueID::from_random(), task_arguments, num_returns,
                                required_resources, timeout_millis);
  auto execution_spec = TaskExecutionSpecification(std::vector<ObjectID>());
  execution_spec.IncrementNumForwards();
  Task task = Task(execution_spec, spec);
  return task;
}


class TimeoutManagerTest : public ::testing::Test {
public:
  TimeoutManagerTest() = default;

protected:
  MockNodeManager mock_node_manager_;
};

TEST_F(TimeoutManagerTest, TestGetTimeoutBudget) {
  // Create an example task.
  auto task = ExampleTask({}, 1, 100);
  auto task_id = task.GetTaskSpecification().TaskId();

  mock_node_manager_.SubmitTask(task, 100, 0);
  mock_node_manager_.ResetTaskUpdateTime(task_id, 50);
  ASSERT_EQ(mock_node_manager_.TimeoutBudgetMillis(task_id), 50);

  mock_node_manager_.ResetTaskUpdateTime(task_id, 100);
  ASSERT_EQ(mock_node_manager_.TimeoutBudgetMillis(task_id), 0);
}

TEST_F(TimeoutManagerTest, TestForwardToOtherNode) {
  // Create an example task.
  auto task = ExampleTask({}, 1, 100);
  auto task_id = task.GetTaskSpecification().TaskId();

  mock_node_manager_.SubmitTask(task, 100, 0);
  mock_node_manager_.ResetTaskUpdateTime(task_id, 30);
  ASSERT_EQ(mock_node_manager_.TimeoutBudgetMillis(task_id), 70);
  ASSERT_TRUE(!mock_node_manager_.Timeout(task_id));

  // Forward to another node.
  MockNodeManager another_node;
  mock_node_manager_.ForwardTaskToOtherNode(task, another_node, 50);
  another_node.ResetTaskUpdateTime(task_id, 100);
  ASSERT_EQ(another_node.TimeoutBudgetMillis(task_id), 20);
  ASSERT_TRUE(!another_node.Timeout(task_id));

  // Spend some millis on another node.
  another_node.ResetTaskUpdateTime(task_id, 200);
  ASSERT_EQ(another_node.TimeoutBudgetMillis(task_id), 0);
  ASSERT_TRUE(another_node.Timeout(task_id));
}

} //namespace raylet

} //namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}