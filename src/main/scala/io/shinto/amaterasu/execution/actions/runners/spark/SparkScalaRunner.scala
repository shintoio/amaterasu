package io.shinto.amaterasu.execution.actions.runners.spark

import java.io.{ ByteArrayOutputStream, File, BufferedReader, PrintWriter }

import io.shinto.amaterasu.configuration.SparkConfig
import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark.repl.{ Main }

import scala.collection.mutable
import scala.io.Source
import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter.IMain

class SparkScalaRunner {

  // This is the amaterasu spark configuration need to rethink the name
  var config: SparkConfig = null
  var actionName: String = null
  var jobId: String = null
  val settings = new Settings()
  var interpreter: IMain = null

  def execute(
    file: String,
    sc: SparkContext,
    actionName: String
  ): Unit = {

    // setting up some context :)
    val sc = createSparkContext()

    interpreter.interpret("var _contextStore = scala.collection.mutable.Map[String, AnyRef]()")
    val contextStore = interpreter.prevRequestList.last.lineRep.call("$result").asInstanceOf[mutable.Map[String, AnyRef]]

    println(contextStore)

    contextStore.put("sc", sc)
    interpreter.interpret("println(_contextStore)")
    println("+++++++++++++++++++++++")
    interpreter.interpret("val sc = _contextStore(\"sc\")")

    // println(binderDefinition)

    for (line <- Source.fromFile(file).getLines()) {

      if (!line.isEmpty) {

        println(line)

        val result = interpreter.interpret(line)

        println("-----------------------------")
        println(result)

      }
    }

    interpreter.close()
  }

  def createSparkContext(): SparkContext = {

    val conf = new SparkConf(true)
      .setMaster(config.master)
      .setAppName(s"${jobId}_$actionName")
      .set("spark.repl.class.uri", Main.getClass().getName) //TODO: :\ check this

    new SparkContext(conf)
  }
}

object SparkScalaRunner {

  def apply(
    config: SparkConfig,
    actionName: String,
    jobId: String
  ): SparkScalaRunner = {

    val result = new SparkScalaRunner()
    result.config = config
    result.actionName = actionName
    result.jobId = jobId

    val interpreter = new IMain()

    //TODO: revisit this, not sure it should be in an apply method
    result.settings.processArguments(List(
      "-Yrepl-class-based",
      "-Yrepl-outdir", s"./",
      "-classpath", interpreter.classLoader.getPackages().mkString(File.pathSeparator)
    ), true)

    result.settings.usejavacp.value = true

    val in: Option[BufferedReader] = null
    val outStream = new ByteArrayOutputStream()
    val out: PrintWriter = new PrintWriter(outStream)
    result.interpreter = new IMain(result.settings, out)
    result
  }
}
