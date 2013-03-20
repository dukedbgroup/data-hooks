package edu.duke.dbmsplus.planner.utils

 /**
 * A class that can be used to calculate more complex stats, like count, average, variance
 */
class Stats(val value: Double = 0) extends Serializable{
  private var n: Long = 0     // Running count of our values
  private var mu: Double = 0  // Running mean of our values
  private var m2: Double = 0  // Running variance numerator (sum of (x - mean)^2)
  
  merge(value)
  
 
  /** Add a value into this StatCounter, updating the internal statistics. */
  def merge(value: Double): Stats = {
    val delta = value - mu
    n += 1
    mu += delta / n
    m2 += delta * (value - mu)
    this
  }
  
  /** Merge another StatCounter into this one, adding up the internal statistics. */
  def merge(other: Stats): Stats = {
    if (other == this) {
      merge(other.copy())  // Avoid overwriting fields in a weird order
    } else {
      val delta = other.mu - mu
      if (other.n * 10 < n) {
        mu = mu + (delta * other.n) / (n + other.n)
      } else if (n * 10 < other.n) {
        mu = other.mu - (delta * n) / (n + other.n)
      } else {
        mu = (mu * n + other.mu * other.n) / (n + other.n)
      }
      m2 += other.m2 + (delta * delta * n * other.n) / (n + other.n)
      n += other.n
      this
    }
  }
 
  /** Clone this StatCounter */
  def copy(): Stats = {
    val other = new Stats
    other.n = n
    other.mu = mu
    other.m2 = m2
    other
  }
  
  def count: Long = n

  def mean: Double = mu

  def sum: Double = n * mu

  /** Return the variance of the values. */
  def variance: Double = {
    if (n == 0)
      Double.NaN
    else
      m2 / n
  }
}