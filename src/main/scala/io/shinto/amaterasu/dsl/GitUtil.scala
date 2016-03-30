package io.shinto.amaterasu.dsl

import java.io.File

import org.eclipse.jgit.api.Git
import org.eclipse.jgit.transport.UsernamePasswordCredentialsProvider

import scala.reflect.io.Path

/**
  * The GitUtil class handles getting the job git repository
  */
object GitUtil {

  /**
    * Clone a repository that requires authentication (username/password). This should
    * be an HTTP URI.
    * @param repoAddress Repository to clone eg. https://
    * @param branch
    * @param user
    * @param password
    * @return
    */
  def cloneRepo(repoAddress: String, branch: String, user: String, password: String) =  {

    val path = Path("repo")
    path.deleteRecursively()
    Git.cloneRepository
      .setURI(repoAddress)
      .setDirectory(new File("repo"))
      .setCredentialsProvider( new UsernamePasswordCredentialsProvider(user, password))
      .setBranch(branch)
      .call
  }


  def cloneRepo(repoAddress: String, branch: String) = {

    val path = Path("repo")
    path.deleteRecursively()
    Git.cloneRepository
      .setURI(repoAddress)
      .setDirectory(new File("repo"))
      .setBranch(branch)
      .call

  }

}