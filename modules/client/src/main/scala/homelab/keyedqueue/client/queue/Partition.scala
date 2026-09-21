package homelab.keyedqueue.client.queue

import homelab.keyedqueue.client.queue.model.{ MessageId, MessageKey }


/**
 * How a value says what to call it and where it belongs.
 *
 * The two things `emit` cannot carry, for a caller that would rather state them once for a type than at
 * every producer it builds.
 *
 * @tparam A what it names
 */
trait Partition[A]:

  /**
   * What a settle will name this message by, and what makes a repeated send one message rather than two.
   *
   * @param value what is being sent
   * @return its id
   */
  def messageId(value: A): MessageId

  /**
   * The key whose order this message takes its place in.
   *
   * @param value what is being sent
   * @return its key
   */
  def messageKey(value: A): MessageKey


object Partition:

  /**
   * The naming a caller has in scope for a type.
   *
   * @tparam A what it names, which needs a partition in scope
   * @return that naming
   */
  def apply[A: Partition as partition]: Partition[A] = partition

  /**
   * A naming stated as one function over the value, for a caller that has not given the type one.
   *
   * @param parts what names a value: its id, and the key whose order it takes its place in
   * @tparam A what it names
   * @return that naming, asking `parts` once per question
   */
  def from[A](parts: A => (MessageId, MessageKey)): Partition[A] = new Partition[A]:
    override def messageId(value: A): MessageId   = parts(value)._1
    override def messageKey(value: A): MessageKey = parts(value)._2
