package io.shinto.amaterasu.common.execution

import java.util.concurrent.LinkedBlockingQueue

import io.shinto.amaterasu.common.configuration.ClusterConfig
import io.shinto.amaterasu.common.dataobjects.ActionData
import io.shinto.amaterasu.enums.ActionStatus
import io.shinto.amaterasu.leader.execution.actions.SequentialAction
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.test.TestingServer
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.zookeeper.CreateMode
import org.scalatest.{FlatSpec, Matchers}

class ActionTests extends FlatSpec with Matchers {

  // setting up a testing zookeeper server (curator TestServer)
  val retryPolicy = new ExponentialBackoffRetry(1000, 3)
  val server = new TestingServer(2181, true)
  val jobId = s"job_${System.currentTimeMillis}"
  val data = ActionData(ActionStatus.pending, "test_action", "start.scala", "spark-scala", null, null)

  "an Action" should "queue it's ActionData int the job queue when executed" in {

    val queue = new LinkedBlockingQueue[ActionData]()
    // val config = ClusterConfig()

    val client = CuratorFrameworkFactory.newClient(server.getConnectString, retryPolicy)
    client.start()

    client.create().withMode(CreateMode.PERSISTENT).forPath(s"/${jobId}")
    val action = SequentialAction(data.name, data.src, data.actionType, jobId, queue, client, 1)

    action.execute()
    queue.peek().name should be(data.name)
    queue.peek().src should be(data.src)

  }

  it should "also create a sequential znode for the task with the value of queued" in {

    val client = CuratorFrameworkFactory.newClient(server.getConnectString, retryPolicy)
    client.start()

    val taskStatus = client.getData.forPath(s"/${jobId}/task-0000000000")

    taskStatus should not be (null)
    new String(taskStatus) should be("queued")

  }

}