package io.shinto.amaterasu.integration

import io.shinto.amaterasu.configuration.ClusterConfig
import io.shinto.amaterasu.dsl.GitUtil
import org.scalatest.{FlatSpec, Matchers}

import scala.reflect.io.{File, Path}
import scala.io.Source

class GitTests extends FlatSpec with Matchers {

  "GitUtil.cloneRepo" should "clone the sample job git repo" in {
    val path = Path("repo")
    File("repo/maki.yaml").exists should be(false)
    try {
      GitUtil.cloneRepo("https://github.com/roadan/amaterasu-job-sample.git", "master")
      val exists = new java.io.File("repo/maki.yaml").exists
      File("repo/maki.yaml").exists should be(true)
    } finally {
      path.deleteRecursively()
    }
  }

  it should "clone the sample job git repo from a repo that requires authentication" in {

    val path = Path("repo")
    File("repo/maki.yaml").exists should be(false)
    val config = ClusterConfig(getClass.getResourceAsStream("/amaterasu.test.properties"))
    try {
      GitUtil.cloneRepo("https://pharma_joe@bitbucket.org/pharma_joe/amaterasu-job-sample.git", "master",
        config.git.user, config.git.password)
      File("repo/maki.yaml").exists should be(true)
    } finally {
      path.deleteRecursively()
    }
  }

}
