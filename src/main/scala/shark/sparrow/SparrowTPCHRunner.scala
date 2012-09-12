package shark.sparrow

import shark.SharkContext
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.TimeUnit
import shark.SharkEnv

/**
 * Given a workload file, runs TPCH queries at a regular rate.
 *
 * Expects MASTER and sparrow.app.name to be in env.
 */
object SparrowTPCHRunner {
  def main(args: Array[String]) {
    if (args.size != 2) {
      println("Expecting file name and query rate in arguments.")
      System.exit(-1);
    }
    val source = scala.io.Source.fromFile(args(0))
    val lines = source.mkString
    source.close()

    val delayMs = args(1).toInt
    val statements = lines.split(";");

    if (statements.size < 6) {
      println("Expecting at least 6 statements to create denorm table")
      System.exit(-1)
    }
    val sc = new SharkContext(System.getenv("MASTER"), "unusedFrameworkName")
    SharkEnv.sc = sc

    val denormCreateStatements = statements.slice(0, 5)
    for (stmt <- denormCreateStatements) {
      sc.sql(stmt)
    }
    val queries = statements.slice(6, statements.length)
    val pool = new ScheduledThreadPoolExecutor(10) // Up to 10 outstanding queries
    var cumulativeDelay = 0;
    for (q <- queries) {
      pool.schedule(new QueryLaunchRunnable(sc, q), cumulativeDelay, TimeUnit.MILLISECONDS)
      cumulativeDelay += delayMs
    }
    println("Finished denormalized table creation")
  }
}

class QueryLaunchRunnable(sc : SharkContext, query: String) extends Runnable {
  def run() {
    sc.sql(query)
  }
}