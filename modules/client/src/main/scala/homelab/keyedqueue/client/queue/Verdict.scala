package homelab.keyedqueue.client.queue

/**
 * What a consumer did with one message of its claim.
 *
 * A claim may be settled piece by piece: naming some messages leaves the rest owed, and the claim ends —
 * releasing the key — once nothing is.
 *
 * @param id which message, named as the delivery named it
 * @param outcome what became of it
 */
final case class Verdict(id: MessageId, outcome: Verdict.Outcome)


object Verdict:

  /**
   * What became of a message.
   */
  enum Outcome:

    /** Worked, and not to be delivered again. */
    case Done

    /** Not worked: it returns to its key's order, behind nothing, to be delivered again. */
    case Failed
