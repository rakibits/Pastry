import akka.actor._
import scala.util.Random
import scala.math._
import scala.Char._
import scala.concurrent.duration._
import scala.collection.mutable._
import scala.language.postfixOps
import scala.concurrent.ExecutionContext.Implicits.global

case object StartTask
case object StartRouting
case object JoinNodesinDT
case object Ack
case object JoinFinish
case object NodeNotFound
case object RoutetoNodeNotFound
case class Task(msg: String, fromID: Int, toID: Int, hops: Int)
case class AddFirstNode(firstEight: ArrayBuffer[Int])
case class AddRow(rowNum: Int, row: Array[Int])
case class AddNodesinNeighhd(nodesSet: ArrayBuffer[Int])
case class SendAcktoMaster(newNodeID: Int)
case class RouteFinish(fromID: Int, toID: Int, hops: Int)

object project3 {
  class Master(totalNodes: Int, numRequests: Int) extends Actor {
    var maxrows = ceil(log(totalNodes.toDouble) / log(4)).toInt
    var maxNodes: Int = pow(4, maxrows).toInt
    var Nodelist = new ArrayBuffer[Int]()
    var GrpOne = new ArrayBuffer[Int]()
    var i: Int = -1
    var numJoined: Int = 0
    var numNotInBoth: Int = 0
    var numRouted: Int = 0
    var numHops: Int = 0
    var numofRouteNotFound: Int = 0

    println("Number of peers: " + totalNodes)
    println("Requests per peer: " + numRequests)

    for (i <- 0 until maxNodes) {
      Nodelist += i
    }
    Nodelist = Random.shuffle(Nodelist)
    GrpOne += Nodelist(0)

    for (i <- 0 until totalNodes) {
      context.actorOf(Props(new PastryActor(totalNodes, numRequests, Nodelist(i), maxrows)),
                      name = String.valueOf(Nodelist(i)))
    }

    def receive = {
      case StartTask =>
        println("\nInitial node created.\nPastry started.\n")
        context.system.actorSelection("/user/master/" + Nodelist(0)) ! AddFirstNode(GrpOne.clone)

      case JoinFinish =>
        numJoined += 1
	    if(numJoined == 1)
          println("Node Join started.")
        if (numJoined > 0) {
          if (numJoined == totalNodes) {
            self ! StartRouting
          } else {
            self ! JoinNodesinDT
          }
        }

      case JoinNodesinDT =>
        val startID = Nodelist(Random.nextInt(numJoined))
        context.system.actorSelection("/user/master/" + startID) !
          Task("Join", startID, Nodelist(numJoined), -1)

      case StartRouting =>
	    println("Node Join Finished.\n")
        println("Routing started.")
        context.system.actorSelection("/user/master/*") ! StartRouting

      case NodeNotFound =>
        numNotInBoth += 1

      case RouteFinish(fromID, toID, hops) =>
        numRouted += 1
        numHops += hops
        if (numRouted >= totalNodes * numRequests) {
          println("Routing finished.\n")
          println("Total number of routes: " + numRouted)
          println("Total number of hops: " + numHops)
          println("Average number of hops per route: " + numHops.toDouble / numRouted.toDouble)

          context.system.shutdown()
        }

      case RoutetoNodeNotFound =>
        numofRouteNotFound += 1
    }
  }

  class PastryActor(numNodes: Int, numRequests: Int, id: Int, maxrows: Int) extends Actor {

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

        CompleteLeafSet(firstGroup)

        for (i <- 0 until maxrows) {
          table(i)(ConvertNumtoBase(currNodeID, maxrows).charAt(i).toString.toInt) = currNodeID
        }

        sender ! JoinFinish

      case Task(msg, fromID, toID, hops) =>
        if (msg == "Join") {

          var samePre = CountMatches(ConvertNumtoBase(currNodeID, maxrows),
                                     ConvertNumtoBase(toID, maxrows))
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
            context.system.actorSelection("/user/master/" + LeftNode.min) !
              Task(msg, fromID, toID, hops + 1)
          } else if (RightNode.length < 4 && RightNode.length > 0 && toID > RightNode.max) {
            context.system.actorSelection("/user/master/" + RightNode.max) !
              Task(msg, fromID, toID, hops + 1)
          } else if ((LeftNode.length == 0 && toID < currNodeID) ||
              (RightNode.length == 0 && toID > currNodeID)) {
            var nodesSet = new ArrayBuffer[Int]()
            nodesSet += currNodeID ++= LeftNode ++= RightNode
            context.system.actorSelection("/user/master/" + toID) !
              AddNodesinNeighhd(nodesSet) //Give leaf set info
          } else if (table(samePre)(ConvertNumtoBase(toID, maxrows).charAt(samePre).toString.toInt) != -1) {
            context.system.actorSelection("/user/master/" + table(samePre)(ConvertNumtoBase(toID, maxrows).charAt(samePre).toString.toInt)) !
              Task(msg, fromID, toID, hops + 1)
          } else if (toID > currNodeID) {
            context.system.actorSelection("/user/master/" + RightNode.max) !
              Task(msg, fromID, toID, hops + 1)
            context.parent ! NodeNotFound
          } else if (toID < currNodeID) {
            context.system.actorSelection("/user/master/" + LeftNode.min) !
              Task(msg, fromID, toID, hops + 1)
            context.parent ! NodeNotFound
          } else {
            println("Impossible!!!")
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
                context.system.actorSelection("/user/master/" + nearest) !
                  Task(msg, fromID, toID, hops + 1)
              } else {
                context.parent ! RouteFinish(fromID, toID, hops + 1)
              }

            } else if (LeftNode.length < 4 && LeftNode.length > 0 && toID < LeftNode.min) {
              context.system.actorSelection("/user/master/" + LeftNode.min) !
                Task(msg, fromID, toID, hops + 1)
            } else if (RightNode.length < 4 && RightNode.length > 0 && toID > RightNode.max) {
              context.system.actorSelection("/user/master/" + RightNode.max) !
                Task(msg, fromID, toID, hops + 1)
            } else if ((LeftNode.length == 0 && toID < currNodeID) ||
                (RightNode.length == 0 && toID > currNodeID)) {
              context.parent ! RouteFinish(fromID, toID, hops + 1)
            } else if (table(samePre)(ConvertNumtoBase(toID, maxrows).charAt(samePre).toString.toInt) != -1) {
              context.system.actorSelection("/user/master/" + table(samePre)(ConvertNumtoBase(toID, maxrows).charAt(samePre).toString.toInt)) ! Task(msg, fromID, toID, hops + 1)
            } else if (toID > currNodeID) {
              context.system.actorSelection("/user/master/" + RightNode.max) !
                Task(msg, fromID, toID, hops + 1)
              context.parent ! RoutetoNodeNotFound
            } else if (toID < currNodeID) {
              context.system.actorSelection("/user/master/" + LeftNode.min) !
                Task(msg, fromID, toID, hops + 1)
              context.parent ! RoutetoNodeNotFound
            } else {
              println("Impossible!!!")
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

      case StartRouting =>
        for (i <- 1 to numRequests)
          context.system.scheduler.scheduleOnce(1000 milliseconds,
                                                self, 
                                                Task("Route", currNodeID, Random.nextInt(IDSpace), -1))

      case message: String =>
        println(s"Unknown message: '$message'")
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
    if (args.length != 2) {
      println("No Argument(s)! Using default mode:")
      BeginPastry(totalNodes = 1000, numRequests = 10)
    } else {
      BeginPastry(totalNodes = args(0).toInt, numRequests = args(1).toInt)
    }

    def BeginPastry(totalNodes: Int, numRequests: Int) {
      val system = ActorSystem("pastry")
      val master = system.actorOf(Props(new Master(totalNodes, numRequests)), name = "master")
      master ! StartTask
    }
  }
}
