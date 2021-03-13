package com.netflix.spinnaker.orca

import com.netflix.spinnaker.orca.api.pipeline.graph.TaskNode.DefinedTask
import com.netflix.spinnaker.orca.api.pipeline.models.StageExecution
import com.netflix.spinnaker.orca.api.pipeline.models.TaskExecution
import com.netflix.spinnaker.orca.pipeline.model.TaskExecutionImpl

/**
 * Resolves the task implementation for a given task node.
 */
interface TaskImplementationResolver {

  fun resolve(stage: StageExecution, taskNode: DefinedTask): TaskExecution {
    return buildTaskExecution(stage, taskNode)
  }

  fun buildTaskExecution(stage: StageExecution, taskNode: DefinedTask): TaskExecution {
    val taskExecution: TaskExecution = TaskExecutionImpl()
    taskExecution.let {
      it.id = (stage.tasks.size + 1).toString()
      it.name = taskNode.name
      it.implementingClass = taskNode.implementingClassName
    }
    return taskExecution
  }

}
