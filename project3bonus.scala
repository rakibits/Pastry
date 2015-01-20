import akka.actor._
import scala.math._
import scala.collection.mutable._
import scala.util.Random
import scala.concurrent.duration._
import scala.Char._
import scala.language.postfixOps
import com.typesafe.config.ConfigFactory

case object StartTask
case object BeginRoute
case object JoinNodesinDT
case object Ack
case object JoinFinish
case object NotInBoth
case object RouteNotInBoth
case object CreateFailures
case object GoDie
case class Task(msg: String, fromID: Int, toID: Int, hops: Int)
case class AddFirstNode(firstEight: ArrayBuffer[Int])
case class AddRow(rowNum: Int, row: Array[Int])
case class AddNodesinNeighhd(nodesSet: ArrayBuffer[Int])
case class SendAcktoMaster(newNodeID: Int)
case class RouteFinish(fromID: Int, toID: Int, hops: Int)
case class RemoveMe(theID: Int)
case class RequestLeafWithout(theID: Int)
case class LeafRecover(newlist: ArrayBuffer[Int], theDead: Int)
case class RequestInTable(row: Int, column: Int)
case class TableRecover(row: Int, column: Int, newID: Int)

object project3bonus {

class Master(totalNodes: Int, totalRequests: Int, numFailures: Int) extends Actor {

  import context._

  var maxrows = ceil(log(totalNodes.toDouble) / log(4)).toInt
  var maxNodes: Int = pow(4, maxrows).toInt
  var Nodelist = new ArrayBuffer[Int]()
  var GrpOne = new ArrayBuffer[Int]()
  var i: Int = -1
  var numJoined: Int = 0
  var numNotInBoth: Int = 0
  var numRouted: Int = 0
  var numHops: Int = 0
  var numRouteNotInBoth: Int = 0

  println("Number of peers: " + totalNodes)
  println("Requests per peer:: " + totalRequests)
  println("Number of inactive peers: " + numFailures)
  if (numFailures >= totalNodes) {
    println("Error! Inactive number of nodes cannot be more than total number of nodes.")
    context.system.shutdown()
  }

  for (i <- 0 until maxNodes) {
    Nodelist += i
  }
  Nodelist = Random.shuffle(Nodelist)

  GrpOne += Nodelist(0)

  for (i <- 0 until totalNodes) {
    context.actorOf(Props(new PastryActor(totalNodes, totalRequests, Nodelist(i), maxrows)), name = String.valueOf(Nodelist(i))) //Create nodes
  }

  def receive = {
    case StartTask =>
      println("\nNode join started.")
      context.system.actorSelection("/user/master/" + Nodelist(0)) ! AddFirstNode(GrpOne.clone)

    case JoinFinish =>
      numJoined += 1
      if (numJoined > 0) {
        if (numJoined == totalNodes) {
          self ! CreateFailures
        } else {
          self ! JoinNodesinDT
        }
      }

    case JoinNodesinDT =>
      val startID = Nodelist(Random.nextInt(numJoined))
      context.system.actorSelection("/user/master/" + startID) ! Task("Join", startID, Nodelist(numJoined), -1)

    case BeginRoute =>
	  println("Node Join Finished.\n")
      println("Routing started.")
      context.system.actorSelection("/user/master/*") ! BeginRoute

    case NotInBoth =>
      numNotInBoth += 1

    case RouteFinish(fromID, toID, hops) =>
      numRouted += 1
      numHops += hops

      if (numRouted == (totalNodes - numFailures) * totalRequests) {
        println("Routing finished.\n")
        println("Total number of routes: " + numRouted)
        println("Total number of hops: " + numHops)
        println("Average number of hops per route:: " + numHops.toDouble / numRouted.toDouble)
        context.system.shutdown()
      }

    case RouteNotInBoth =>
      numRouteNotInBoth += 1

    case CreateFailures =>
      for (i <- 0 until numFailures)
        context.system.actorSelection("/user/master/" + Nodelist(Random.nextInt(numJoined))) ! GoDie
      context.system.scheduler.scheduleOnce(1000 milliseconds, self, BeginRoute)
  }

}

class PastryActor(totalNodes: Int, totalRequests: Int, id: Int, maxrows: Int) extends Actor {

  import context._

  val currNodeID = id;
  var LeftNode = new ArrayBuffer[Int]()
  var RightNode = new ArrayBuffer[Int]()
  var table = new Array[Array[Int]](maxrows)
  var numOfBack: Int = 0
  val IDSpace: Int = pow(4, maxrows).toInt

  var i = 0
  for (i <- 0 until maxrows)
    table(i) = Array(-1, -1, -1, -1)

  def receive = {

    case AddFirstNode(firstGroup) =>
      firstGroup -= currNodeID

      for (i <- 0 until maxrows) {
        table(i)(ConvertNumtoBase(currNodeID, maxrows).charAt(i).toString.toInt) = currNodeID
      }

      sender ! JoinFinish

    case Task(msg, fromID, toID, hops) =>
      if (msg == "Join") {
        var samePre = CountMatches(ConvertNumtoBase(currNodeID, maxrows), ConvertNumtoBase(toID, maxrows))
        if (hops == -1 && samePre > 0) {
          for (i <- 0 until samePre) {
            context.system.actorSelection("/user/master/" + toID) ! AddRow(i, table(i).clone)
          }
        }
        context.system.actorSelection("/user/master/" + toID) ! AddRow(samePre, table(samePre).clone)

        if ((LeftNode.length > 0 && toID >= LeftNode.min && toID <= currNodeID) ||
          (RightNode.length > 0 && toID <= RightNode.max && toID >= currNodeID)) {
          var diff = IDSpace + 10
          var nearest = -1
          if (toID < currNodeID) {
            for (i <- LeftNode) {
              if (abs(toID - i) < diff) {
                nearest = i
                diff = abs(toID - i)
              }
            }
          } else {
            for (i <- RightNode) {
              if (abs(toID - i) < diff) {
                nearest = i
                diff = abs(toID - i)
              }
            }
          }

          if (abs(toID - currNodeID) > diff) {
            context.system.actorSelection("/user/master/" + nearest) ! Task(msg, fromID, toID, hops + 1)
          } else {
            var nodesSet = new ArrayBuffer[Int]()
            nodesSet += currNodeID ++= LeftNode ++= RightNode
            context.system.actorSelection("/user/master/" + toID) ! AddNodesinNeighhd(nodesSet)
          }

        } else if (LeftNode.length < 4 && LeftNode.length > 0 && toID < LeftNode.min) {
          context.system.actorSelection("/user/master/" + LeftNode.min) ! Task(msg, fromID, toID, hops + 1)
        } else if (RightNode.length < 4 && RightNode.length > 0 && toID > RightNode.max) {
          context.system.actorSelection("/user/master/" + RightNode.max) ! Task(msg, fromID, toID, hops + 1)
        } else if ((LeftNode.length == 0 && toID < currNodeID) || (RightNode.length == 0 && toID > currNodeID)) {
          var nodesSet = new ArrayBuffer[Int]()
          nodesSet += currNodeID ++= LeftNode ++= RightNode
          context.system.actorSelection("/user/master/" + toID) ! AddNodesinNeighhd(nodesSet)
        } else if (table(samePre)(ConvertNumtoBase(toID, maxrows).charAt(samePre).toString.toInt) != -1) {
          context.system.actorSelection("/user/master/" + table(samePre)(ConvertNumtoBase(toID, maxrows).charAt(samePre).toString.toInt)) ! Task(msg, fromID, toID, hops + 1)
        } else {
          var diff = IDSpace + 10
          var nearest = -1
          for (i <- 0 until 4) {
            if ((table(samePre)(i) != -1) && (abs(table(samePre)(i) - toID) < diff)) {
              diff = abs(table(samePre)(i) - toID)
              nearest = table(samePre)(i)
            }
          }
          if (nearest != -1) {
            if (nearest == currNodeID) {
              if (toID > currNodeID) {
                context.system.actorSelection("/user/master/" + RightNode.max) ! Task(msg, fromID, toID, hops + 1)
                context.parent ! NotInBoth
              } else if (toID < currNodeID) {
                context.system.actorSelection("/user/master/" + LeftNode.min) ! Task(msg, fromID, toID, hops + 1)
                context.parent ! NotInBoth
              } else {
                println("NO!")
              }
            } else {
              context.system.actorSelection("/user/master/" + nearest) ! Task(msg, fromID, toID, hops + 1)
            }
          }
        }

      } else if (msg == "Route") { 
        if (currNodeID == toID) {
          context.parent ! RouteFinish(fromID, toID, hops + 1)
        } else {
          var samePre = CountMatches(ConvertNumtoBase(currNodeID, maxrows), ConvertNumtoBase(toID, maxrows))

          if ((LeftNode.length > 0 && toID >= LeftNode.min && toID < currNodeID) ||
            (RightNode.length > 0 && toID <= RightNode.max && toID > currNodeID)) {
            var diff = IDSpace + 10
            var nearest = -1
            if (toID < currNodeID) {
              for (i <- LeftNode) {
                if (abs(toID - i) < diff) {
                  nearest = i
                  diff = abs(toID - i)
                }
              }
            } else {
              for (i <- RightNode) {
                if (abs(toID - i) < diff) {
                  nearest = i
                  diff = abs(toID - i)
                }
              }
            }

            if (abs(toID - currNodeID) > diff) {
              context.system.actorSelection("/user/master/" + nearest) ! Task(msg, fromID, toID, hops + 1)
            } else {
              context.parent ! RouteFinish(fromID, toID, hops + 1)
            }
          } else if (LeftNode.length < 4 && LeftNode.length > 0 && toID < LeftNode.min) {
            context.system.actorSelection("/user/master/" + LeftNode.min) ! Task(msg, fromID, toID, hops + 1)
          } else if (RightNode.length < 4 && RightNode.length > 0 && toID > RightNode.max) {
            context.system.actorSelection("/user/master/" + RightNode.max) ! Task(msg, fromID, toID, hops + 1)
          } else if ((LeftNode.length == 0 && toID < currNodeID) || (RightNode.length == 0 && toID > currNodeID)) {
            context.parent ! RouteFinish(fromID, toID, hops + 1)
          } else if (table(samePre)(ConvertNumtoBase(toID, maxrows).charAt(samePre).toString.toInt) != -1) {
            context.system.actorSelection("/user/master/" + table(samePre)(ConvertNumtoBase(toID, maxrows).charAt(samePre).toString.toInt)) ! Task(msg, fromID, toID, hops + 1)
          } else {
            var diff = IDSpace + 10
            var nearest = -1
            for (i <- 0 until 4) {
              if ((table(samePre)(i) != -1) && (abs(table(samePre)(i) - toID) < diff)) {
                diff = abs(table(samePre)(i) - toID)
                nearest = table(samePre)(i)
              }
            }
            if (nearest != -1) {
              if (nearest == currNodeID) {
                if (toID > currNodeID) {
                  context.system.actorSelection("/user/master/" + RightNode.max) ! Task(msg, fromID, toID, hops + 1)
                  context.parent ! RouteNotInBoth
                } else if (toID < currNodeID) {
                  context.system.actorSelection("/user/master/" + LeftNode.min) ! Task(msg, fromID, toID, hops + 1)
                  context.parent ! RouteNotInBoth
                }
              } else {
                context.system.actorSelection("/user/master/" + nearest) ! Task(msg, fromID, toID, hops + 1)
              }
            }
          }
        }
      }

    case AddRow(rowNum, newRow) =>
      for (i <- 0 until 4)
        if (table(rowNum)(i) == -1)
          table(rowNum)(i) = newRow(i)

    case AddNodesinNeighhd(nodesSet) =>
      CompleteLeafSet(nodesSet)
      for (i <- LeftNode) {
        numOfBack += 1
        context.system.actorSelection("/user/master/" + i) ! SendAcktoMaster(currNodeID)
      }
      for (i <- RightNode) {
        numOfBack += 1
        context.system.actorSelection("/user/master/" + i) ! SendAcktoMaster(currNodeID)
      }
      for (i <- 0 until maxrows) {
        var j = 0
        for (j <- 0 until 4)
          if (table(i)(j) != -1) {
            numOfBack += 1
            context.system.actorSelection("/user/master/" + table(i)(j)) ! SendAcktoMaster(currNodeID)
          }
      }
      for (i <- 0 until maxrows) {
        table(i)(ConvertNumtoBase(currNodeID, maxrows).charAt(i).toString.toInt) = currNodeID
      }

    case SendAcktoMaster(newNodeID) =>
      addOne(newNodeID)
      sender ! Ack

    case Ack =>
      numOfBack -= 1
      if (numOfBack == 0)
        context.parent ! JoinFinish

    case BeginRoute =>
      for (i <- 1 to totalRequests)
        context.system.scheduler.scheduleOnce(1000 milliseconds, self, Task("Route", currNodeID, Random.nextInt(totalNodes), -1))
        
    case GoDie =>
      context.system.actorSelection("/user/master/*") ! RemoveMe(currNodeID)
      context.stop(self)

    case RemoveMe(theID) =>
      if (theID > currNodeID && RightNode.contains(theID)) {
        RightNode -= theID
        if (RightNode.length > 0)
          context.system.actorSelection("/user/master/" + RightNode.max) ! RequestLeafWithout(theID)
      }
      if (theID < currNodeID && LeftNode.contains(theID)) {
        LeftNode -= theID
        if (LeftNode.length > 0)
          context.system.actorSelection("/user/master/" + LeftNode.min) ! RequestLeafWithout(theID)
      }
      var samePre = CountMatches(ConvertNumtoBase(currNodeID, maxrows), ConvertNumtoBase(theID, maxrows))
      if (table(samePre)(ConvertNumtoBase(theID, maxrows).charAt(samePre).toString.toInt) == theID) {
        table(samePre)(ConvertNumtoBase(theID, maxrows).charAt(samePre).toString.toInt) = -1
        for (i <- 0 until 4) {
          if (table(samePre)(i) != currNodeID && table(samePre)(i) != theID && table(samePre)(i) != -1) {
            context.system.actorSelection("/user/master/" + table(samePre)(i)) !
              RequestInTable(samePre, ConvertNumtoBase(theID, maxrows).charAt(samePre).toString.toInt)
          }
        }
      }

    case RequestLeafWithout(theID) =>
      var temp = new ArrayBuffer[Int]()
      temp ++= LeftNode ++= RightNode -= theID
      sender ! LeafRecover(temp.clone, theID)

    case LeafRecover(newlist, theDead) =>
      for (i <- newlist) {
        if(RightNode.contains(theDead))
        {
          RightNode = RightNode-theDead
        }
        if(LeftNode.contains(theDead))
        {
          LeftNode = LeftNode-theDead
        }
        if (i > currNodeID && !RightNode.contains(i)) {
          if (RightNode.length < 4) {
            RightNode += i
          } else {
            if (i < RightNode.max) {
              RightNode += i
            }
          }
        } else if (i < currNodeID && !LeftNode.contains(i)) {
          if (LeftNode.length < 4) {
            LeftNode += i
          } else {
            if (i > LeftNode.min) {
              LeftNode += i
            }
          }
        }
      }

    case RequestInTable(samePre, column) =>
      if (table(samePre)(column) != -1)
        sender ! TableRecover(samePre, column, table(samePre)(column))

    case TableRecover(row, column, newID) =>
      if (table(row)(column) == -1) {
        table(row)(column) = newID
      }
  }

  def ConvertNumtoBase(raw: Int, length: Int): String = {
    var str: String = Integer.toString(raw, 4)
    val diff: Int = length - str.length()
    if (diff > 0) {
      var j = 0
      while (j < diff) {
        str = '0' + str
        j += 1
      }
    }
    return str
  }

  def CountMatches(str1: String, str2: String): Int = {
    var j = 0
    while (j < str1.length && str1.charAt(j).equals(str2.charAt(j))) {
      j += 1
    }
    return j
  }

  def CompleteLeafSet(all: ArrayBuffer[Int]): Unit = {
    for (i <- all) {
      if (i > currNodeID && !RightNode.contains(i)) {
        if (RightNode.length < 4) {
          RightNode += i
        } else {
          if (i < RightNode.max) {
            RightNode -= RightNode.max
            RightNode += i
          }
        }
      } else if (i < currNodeID && !LeftNode.contains(i)) {
        if (LeftNode.length < 4) {
          LeftNode += i
        } else {
          if (i > LeftNode.min) {
            LeftNode -= LeftNode.min
            LeftNode += i
          }
        }
      }
      var samePre = CountMatches(ConvertNumtoBase(currNodeID, maxrows), ConvertNumtoBase(i, maxrows))
      if (table(samePre)(ConvertNumtoBase(i, maxrows).charAt(samePre).toString.toInt) == -1) {
        table(samePre)(ConvertNumtoBase(i, maxrows).charAt(samePre).toString.toInt) = i
      }
    }
  }

  def addOne(one: Int): Unit = {
    if (one > currNodeID && !RightNode.contains(one)) {
      if (RightNode.length < 4) {
        RightNode += one
      } else {
        if (one < RightNode.max) {
          RightNode -= RightNode.max
          RightNode += one
        }
      }
    } else if (one < currNodeID && !LeftNode.contains(one)) {
      if (LeftNode.length < 4) {
        LeftNode += one
      } else {
        if (one > LeftNode.min) {
          LeftNode -= LeftNode.min
          LeftNode += one
        }
      }
    }
    var samePre = CountMatches(ConvertNumtoBase(currNodeID, maxrows), ConvertNumtoBase(one, maxrows))
    if (table(samePre)(ConvertNumtoBase(one, maxrows).charAt(samePre).toString.toInt) == -1) {
      table(samePre)(ConvertNumtoBase(one, maxrows).charAt(samePre).toString.toInt) = one
    }
  }
}
  def main(args: Array[String]) {
    if (args.length == 3) {
      BeginPastry(totalNodes = args(0).toInt, totalRequests = args(1).toInt, numFailures = args(2).toInt) //User Specified Mode
    } else if (args.length == 2) {
      BeginPastry(totalNodes = args(0).toInt, totalRequests = args(1).toInt, numFailures = args(0).toInt / 100)
    } else {
      println("No Argument(s)! Using default mode:")
      BeginPastry(totalNodes = 100, totalRequests = 10, numFailures = 10)
    }

    def BeginPastry(totalNodes: Int, totalRequests: Int, numFailures: Int) {
      val config = ConfigFactory.parseString(
                        """akka{
                        stdout-loglevel = "OFF"
                        loglevel = "OFF"
                        }""")
      val system = ActorSystem("pastry", ConfigFactory.load(config))
      val master = system.actorOf(Props(new Master(totalNodes, totalRequests, numFailures)), name = "master")
      master ! StartTask
    }
  }
}