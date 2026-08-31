package homelab.keyedqueue.domain.model

import zio.Chunk

/**
 * What a consumer says it still holds, sorted into what this service issued and what it did not.
 *
 * '''The trusted counterpart of `QueueRequest.Heartbeat`, and the one that cannot fail.''' The other three
 * parses refuse something; this one refuses nothing, because a receipt it cannot read is not an error here
 * — a heartbeat carries many, and failing the call over one would cost the consumer the renewals that were
 * good. So the unreadable ones are carried through to be reported as lost, which is what the consumer must
 * treat them as anyway.
 *
 * That is why this is a partition rather than a validation, and why the parse that produces it returns it
 * directly rather than wrapped in `Validated`.
 *
 * @param held the claims the receipts named, to be renewed
 * @param unreadable the receipts that named none, echoed back as the consumer sent them
 */
final case class Renewal(held: Chunk[Claim], unreadable: Chunk[String])
