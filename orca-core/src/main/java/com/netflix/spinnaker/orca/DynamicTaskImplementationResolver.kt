package com.netflix.spinnaker.orca

import com.netflix.spinnaker.kork.dynamicconfig.DynamicConfigService
import com.netflix.spinnaker.orca.api.pipeline.Task
import com.netflix.spinnaker.orca.api.pipeline.graph.TaskNode.TaskDefinition
import com.netflix.spinnaker.orca.api.pipeline.graph.TaskNode.DefinedTask
import com.netflix.spinnaker.orca.api.pipeline.models.StageExecution
import com.netflix.spinnaker.orca.api.pipeline.models.TaskExecution
import com.netflix.spinnaker.orca.clouddriver.utils.CloudProviderAware
import com.netflix.spinnaker.orca.config.TaskOverrideConfigurationProperties
import com.netflix.spinnaker.orca.config.TaskOverrideConfigurationProperties.TaskOverrideDefinition
import org.slf4j.LoggerFactory

/**
 * This resolver will modify the task definition if the original definition
 * matches the criteria for task replacement and the configuration for such a replacement is enabled.
 */
class DynamicTaskImplementationResolver(
  val dynamicConfigService: DynamicConfigService,
  val taskOverrideConfigurationProperties: TaskOverrideConfigurationProperties?,
  ): TaskImplementationResolver, CloudProviderAware {

  private val log = LoggerFactory.getLogger(javaClass)

  override fun resolve(stage: StageExecution, taskNode: DefinedTask): TaskExecution {
    val taskOverrideDefinition = taskOverrideConfigurationProperties
      ?.overrideDefinitions
      ?.firstOrNull {
        it.stageName.equals(stage.type, true) &&
          it.originalTaskName.equals((taskNode as DefinedTask).name, true)
      } ?: return buildTaskExecution(stage, taskNode)

    if (isOverrideEnabled(getConfigAttributeName(stage, taskOverrideDefinition))) {
      val clazz: Class<*> = Class.forName(taskOverrideDefinition.newTaskImplementationClassName)
      if (clazz.interfaces.contains(Task::class.java)) {
        val newTask: DefinedTask = TaskDefinition(
          taskOverrideDefinition.newTaskName,
          clazz as Class<Task>
        )
        log.info("Task '{}' is overridden with new task impl class '{}'",
          taskNode.name,
          taskOverrideDefinition.newTaskImplementationClassName
        )
        return buildTaskExecution(stage, newTask)
      }
      log.warn("Task '{}' is overridden but the new task impl class '{}' is not of type task",
        taskNode.name,
        taskOverrideDefinition.newTaskImplementationClassName)
    }
    log.info("No task override set {}", taskNode.name)
    return buildTaskExecution(stage, taskNode)

  }

  private fun getConfigAttributeName(stage: StageExecution,
                                     taskOverrideDefinition: TaskOverrideDefinition
  ): String {
    val configAttributeParts: MutableList<String> = mutableListOf()
    taskOverrideDefinition.overrideCriteriaAttributes?.forEach {
      when (it) {
        APPLICATION_ATTR_NAME -> configAttributeParts.add(stage.execution.application)
        CLOUDPROVIDER_ATTR_NAME ->
          configAttributeParts.add(getCloudProvider(stage) ?: CloudProviderAware.DEFAULT_CLOUD_PROVIDER)
        else -> {
          if (stage.context[it] != null) {
            configAttributeParts.add(stage.context[it].toString())
          }
        }
      }
    }

    configAttributeParts.add(stage.type.toLowerCase())

    return configAttributeParts.joinToString(".", ATTRIBUTE_PREFIX)
  }

  private fun isOverrideEnabled(configAttrName: String): Boolean {
    return dynamicConfigService.isEnabled(configAttrName, false)
  }

  private companion object {
    const val ATTRIBUTE_PREFIX = "task-override."
    const val APPLICATION_ATTR_NAME = "application"
    const val CLOUDPROVIDER_ATTR_NAME = "cloudprovider"
  }

}
