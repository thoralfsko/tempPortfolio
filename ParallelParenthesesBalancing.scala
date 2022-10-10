package reductions

import scala.annotation.*
import org.scalameter.*

object ParallelParenthesesBalancingRunner:

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns := 40,
    Key.exec.maxWarmupRuns := 80,
    Key.exec.benchRuns := 120,
    Key.verbose := false
  ) withWarmer(Warmer.Default())

  def main(args: Array[String]): Unit =
    val length = 100000000
    val chars = new Array[Char](length)
    val threshold = 10000
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime")
    println(s"speedup: ${seqtime.value / fjtime.value}")

object ParallelParenthesesBalancing extends ParallelParenthesesBalancingInterface:

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def balance(chars: Array[Char]): Boolean =
    @tailrec
    def loop(chars: Array[Char], x: Int): Boolean = 
      if chars.isEmpty then if x == 0 then true else false 
      else if x < 0 then false 
      else if chars.head == '(' then loop(chars.tail, x + 1)
      else if chars.head == ')' then loop(chars.tail, x - 1)
      else loop(chars.tail, x)

    loop(chars, 0)

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def parBalance(chars: Array[Char], threshold: Int): Boolean =

    def traverse(idx: Int, until: Int, diff: Int, minDiff: Int) : (Int, Int) = {
      if idx == until then (diff, minDiff)
      else if chars(idx) == '(' then traverse(idx + 1, until, diff + 1, minDiff)
      else if chars(idx) == ')' then 
        traverse(idx + 1, until, diff - 1, if minDiff > diff then diff else minDiff)
      else traverse(idx + 1, until, diff, minDiff)
    }

    def reduce(from: Int, until: Int): (Int, Int) = {
      if until - from < threshold then traverse(from, until, 0, 0)
      else
        val midpoint = (until - from) / 2

        val (left, right) = 
          parallel(
            reduce(from, from + midpoint),
            reduce(from + midpoint, until)
          )

        op(left, right)
    }

    def op(left: (Int, Int), right: (Int, Int)): (Int, Int) = 
      val (ld, lmd) = left // lrft delta, left min delta
      val (rd, rmd) = right // right delta, right min delta
      if lmd < rmd then (ld + rd, lmd)
      else (ld + rd, ld - rmd)

    reduce(0, chars.length) == (0, 0)

  // For those who want more:
  // Prove that your reduction operator is associative!

